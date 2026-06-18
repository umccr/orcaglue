import json

import pulumi
import pulumi_aws as aws
from orcaglue_shared_lib.tagging import register_global_tags

register_global_tags()

db_name = "orcavault"
stack_prefix = "orcaglue"
stack_stage = pulumi.get_stack()  # dev or prod

config = pulumi.Config()

lz_bucket = config.require("lz-bucket")
rs_workgroup_name = config.require("rs-workgroup")

# --- Look up pre-existing resources ---

landing_zone_bucket = aws.s3.get_bucket(bucket=lz_bucket)

rs_workgroup = aws.redshiftserverless.get_workgroup(workgroup_name=rs_workgroup_name)

# --- 1. Define the Shared IAM Assume Role Policy for Glue ---

glue_assume_role_policy = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Action": "sts:AssumeRole",
                "Effect": "Allow",
                "Principal": {"Service": "glue.amazonaws.com"},
            }
        ],
    }
)

# --- 2. Create the Shared IAM Role ---

shared_glue_role = aws.iam.Role(
    f"{stack_prefix}-shared-infra-glue-job-role-{stack_stage}",
    name=f"{stack_prefix}-shared-infra-glue-job-role-{stack_stage}",
    assume_role_policy=glue_assume_role_policy,
)

# --- 3. Attach AWS Managed Policy for Glue Service ---

role_policy_attachment = aws.iam.RolePolicyAttachment(
    f"{stack_prefix}-shared-infra-glue-job-role-policy-attachment-{stack_stage}",
    role=shared_glue_role.name,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
)

# --- 4. Inline Policy: S3, SSM, Redshift Data API ---

glue_inline_policy = aws.iam.RolePolicy(
    f"{stack_prefix}-shared-infra-glue-job-role-inline-policy-{stack_stage}",
    role=shared_glue_role.id,
    policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                # S3 permissions on landing zone bucket
                {
                    "Sid": "LandingZoneS3Access",
                    "Effect": "Allow",
                    "Action": sorted(
                        [
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:ListBucket",
                        ]
                    ),
                    "Resource": sorted(
                        [
                            landing_zone_bucket.arn,
                            f"{landing_zone_bucket.arn}/*",
                        ]
                    ),
                },
                # SSM Parameter Store — Google Drive credentials
                {
                    "Sid": "SSMParameterAccess",
                    "Effect": "Allow",
                    "Action": sorted(
                        [
                            "ssm:GetParameter",
                            "ssm:GetParameters",
                            "ssm:GetParametersByPath",
                        ]
                    ),
                    "Resource": ["*"],
                },
                # Redshift Data API — ExecuteStatement scoped to workgroup
                {
                    "Sid": "RedshiftDataAPIExecute",
                    "Effect": "Allow",
                    "Action": [
                        "redshift-data:ExecuteStatement",
                    ],
                    "Resource": [rs_workgroup.arn],
                },
                # Redshift Data API — DescribeStatement must be wildcard
                {
                    "Sid": "RedshiftDataAPIDescribe",
                    "Effect": "Allow",
                    "Action": [
                        "redshift-data:DescribeStatement",
                    ],
                    "Resource": ["*"],
                },
                # Redshift Serverless — scoped to workgroup
                {
                    "Sid": "RedshiftServerlessAccess",
                    "Effect": "Allow",
                    "Action": [
                        "redshift-serverless:GetCredentials",
                    ],
                    "Resource": [rs_workgroup.arn],
                },
            ],
        }
    ),
)

# --- 5. Grant Redshift schema privileges to the Glue execution role ---

# Caveat
# aws.redshiftdata.Statement is a one-shot execution resource — Pulumi runs it once on creation.
# It will not re-run on subsequent pulumi up unless the resource is replaced.
# This is fine for GRANT statements since they are idempotent in Redshift — running them again does no harm.
#
# If you ever need to force re-run:
#
#   pulumi stack --show-urns
#   pulumi up --replace 'urn:pulumi:dev::shared-infra::aws:redshiftdata/statement:Statement::orcaglue-shared-infra-glue-role-tsa-grants-dev'

# --- 5. Grant Redshift schema privileges to the Glue execution role ---

create_schema = aws.redshiftdata.Statement(
    f"{stack_prefix}-shared-infra-glue-role-tsa-create-schema-{stack_stage}",
    workgroup_name=rs_workgroup_name,
    database=db_name,
    sql="CREATE SCHEMA IF NOT EXISTS tsa;",
    opts=pulumi.ResourceOptions(depends_on=[shared_glue_role]),
)

glue_role_redshift_grants = aws.redshiftdata.Statement(
    f"{stack_prefix}-shared-infra-glue-role-tsa-grants-{stack_stage}",
    workgroup_name=rs_workgroup_name,
    database=db_name,
    sql=shared_glue_role.name.apply(
        lambda role_name: "\n".join(
            [
                f'GRANT USAGE ON SCHEMA tsa TO "IAMR:{role_name}";',
                f'GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA tsa TO "IAMR:{role_name}";',
                f'ALTER DEFAULT PRIVILEGES IN SCHEMA tsa GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO "IAMR:{role_name}";',
            ]
        )
    ),
    opts=pulumi.ResourceOptions(depends_on=[create_schema]),
)

# --- 6. Export role ARN for cross-stack reference ---

pulumi.export("shared_glue_role_arn", shared_glue_role.arn)
