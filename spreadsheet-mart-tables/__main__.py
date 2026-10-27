import hashlib
import json
import os

import pulumi
import pulumi_aws as aws
from orcaglue_shared_lib.tagging import register_global_tags

register_global_tags()

stack_prefix = "orcaglue"
stack_stage = pulumi.get_stack()  # dev or prod

config = pulumi.Config()

requirements = config.require("requirements")
job_script = config.require("job-script")
lz_bucket = config.require("lz-bucket")
source_bucket = config.require("source-bucket")
source_prefix = config.require("source-prefix")
aurora_cluster_id = config.require("aurora-cluster")
aurora_database = config.get("aurora-database") or "orcanexus"
aurora_secret_name = config.require("aurora-secret")
mart_tables = config.require("mart-tables")
shared_infra_path = config.require("shared-infra")
# Default is 18:00 UTC = 04:00 AEST / 05:00 AEDT. Set it after the Redshift UNLOAD.
schedule = config.get("schedule") or "cron(0 18 * * ? *)"
trigger_enabled = config.get_bool("trigger-enabled")

shared_infra_ref = pulumi.StackReference(shared_infra_path)
shared_glue_role_arn = shared_infra_ref.get_output("shared_glue_role_arn")

pulumi.export("shared_glue_role_arn", shared_glue_role_arn)

# ---

# Look up pre-existing resources
landing_zone_bucket = aws.s3.get_bucket(bucket=lz_bucket)

# Created and managed by orcahouse infra/s3/orcavault (the Redshift UNLOAD target).
source_bucket_info = aws.s3.get_bucket(bucket=source_bucket)

# Created and managed by orcahouse infra/aurora.
aurora_cluster = aws.rds.get_cluster(cluster_identifier=aurora_cluster_id)

# Created by orcahouse infra/aurora/dba/04_rw.sql. The Data API calls run as this login.
aurora_secret = aws.secretsmanager.get_secret(name=aurora_secret_name)

job_name = "spreadsheet-mart-tables"

# --- IAM: this module's access, attached to the shared Glue role ---

# The shared role and its common permissions belong to shared-infra. This job's own access
# is an extra inline policy on that role, owned by this stack, so it is deployed and destroyed
# with the job. The role name is the last part of the shared role ARN.
shared_glue_role_name = shared_glue_role_arn.apply(lambda arn: arn.split("/")[-1])

mart_import_policy = aws.iam.RolePolicy(
    "mart-import-policy",
    name=f"{stack_prefix}-{stack_stage}-{job_name}-policy",
    role=shared_glue_role_name,
    policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                # S3 list on the mart CSV unload prefix, to find the UNLOAD manifests
                {
                    "Sid": "MartSourceS3List",
                    "Effect": "Allow",
                    "Action": ["s3:ListBucket"],
                    "Resource": [source_bucket_info.arn],
                    "Condition": {"StringLike": {"s3:prefix": [f"{source_prefix}*"]}},
                },
                # S3 read the manifests, and rewrite the parts in place with
                # Content-Encoding: gzip, which the Aurora aws_s3 import requires
                {
                    "Sid": "MartSourceS3Objects",
                    "Effect": "Allow",
                    "Action": sorted(
                        [
                            "s3:GetObject",
                            "s3:PutObject",
                        ]
                    ),
                    "Resource": [f"{source_bucket_info.arn}/{source_prefix}*"],
                },
                # RDS Data API — scoped to the Aurora cluster
                {
                    "Sid": "AuroraDataAPI",
                    "Effect": "Allow",
                    "Action": sorted(
                        [
                            "rds-data:BeginTransaction",
                            "rds-data:CommitTransaction",
                            "rds-data:ExecuteStatement",
                            "rds-data:RollbackTransaction",
                        ]
                    ),
                    "Resource": [aurora_cluster.arn],
                },
                # The database login secret the Data API calls run as
                {
                    "Sid": "AuroraSecretAccess",
                    "Effect": "Allow",
                    "Action": ["secretsmanager:GetSecretValue"],
                    "Resource": [aurora_secret.arn],
                },
            ],
        }
    ),
)

pulumi.export("mart_import_policy_name", mart_import_policy.name)

# --- S3 Uploads ---

# NOTE: this is intended naming convention, i.e. <datasource>__<suffix_meaningful_naming_convention>
# The load targets are the Aurora mart tables; the base name keys the job files and artifacts.
base_name = "spreadsheet__mart_tables"
s3_mid_path = f"{stack_prefix}/{base_name}/{stack_stage}"


def file_md5(path: str) -> str:
    with open(os.path.abspath(path), "rb") as handle:
        return hashlib.md5(handle.read()).hexdigest()


# Upload requirements.txt
requirements_s3 = aws.s3.BucketObject(
    "requirements-txt",
    bucket=landing_zone_bucket.bucket,
    key=f"{s3_mid_path}/requirements.txt",
    source=pulumi.FileAsset(requirements),
    etag=file_md5(requirements),
)

pulumi.export("requirements_s3_key", requirements_s3.key)

# Upload job script
job_script_s3 = aws.s3.BucketObject(
    "job-script",
    bucket=landing_zone_bucket.bucket,
    key=f"{s3_mid_path}/{os.path.basename(job_script)}",
    source=pulumi.FileAsset(job_script),
    etag=file_md5(job_script),
)

pulumi.export("job_script_s3_key", job_script_s3.key)

# --- Glue Job ---

glue_job = aws.glue.Job(
    "glue-job",
    name=f"{stack_prefix}-{stack_stage}-{job_name}-job",
    role_arn=shared_glue_role_arn,
    glue_version="5.0",
    worker_type="G.1X",
    number_of_workers=2,
    timeout=30,
    command=aws.glue.JobCommandArgs(
        name="glueetl",
        script_location=pulumi.Output.all(
            landing_zone_bucket.bucket,
            job_script_s3.key,
        ).apply(lambda args: f"s3://{args[0]}/{args[1]}"),
        python_version="3",
    ),
    default_arguments=pulumi.Output.all(
        landing_zone_bucket.bucket,
        requirements_s3.key,
    ).apply(
        lambda args: {
            "--job-language": "python",
            "--python-modules-installer-option": "-r",
            "--additional-python-modules": f"s3://{args[0]}/{args[1]}",
            "--lz_bucket": args[0],
            "--base_name": base_name,
            "--s3_mid_path": s3_mid_path,
            "--source_bucket": source_bucket,
            "--source_prefix": source_prefix,
            "--aurora_cluster_arn": aurora_cluster.arn,
            "--aurora_secret_arn": aurora_secret.arn,
            "--aurora_database": aurora_database,
            "--mart_tables": mart_tables,
            "--allow_empty": "false",
            "--dry_run": "false",
        }
    ),
    # A run must not start before the job's access exists.
    opts=pulumi.ResourceOptions(depends_on=[mart_import_policy]),
)

pulumi.export("glue_job_name", glue_job.name)
pulumi.export("source_uri", f"s3://{source_bucket}/{source_prefix}")
pulumi.export("target_schema", f"{aurora_cluster_id}/{aurora_database}.mart")
pulumi.export("mart_tables", mart_tables)

# --- Glue Trigger ---

# Keep triggers opt-in so a stage can be validated manually before scheduling.
enable_trigger = trigger_enabled if trigger_enabled is not None else False

glue_trigger = aws.glue.Trigger(
    "glue-trigger",
    name=pulumi.Output.from_input(glue_job.name).apply(
        lambda name: f"{name}-scheduled-trigger"
    ),
    type="SCHEDULED",
    schedule=schedule,
    description=pulumi.Output.from_input(glue_job.name).apply(
        lambda name: f"Daily trigger for {name}"
    ),
    enabled=enable_trigger,
    start_on_creation=enable_trigger,
    actions=[
        aws.glue.TriggerActionArgs(
            job_name=glue_job.name,
        )
    ],
)

pulumi.export("glue_trigger_name", glue_trigger.name)
pulumi.export("glue_trigger_enabled", enable_trigger)
