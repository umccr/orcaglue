import glob
import hashlib
import json
import os
import re

import pulumi
import pulumi_aws as aws
from orcaglue_shared_lib.tagging import register_global_tags

register_global_tags()

stack_prefix = "orcaglue"
stack_stage = pulumi.get_stack()  # dev or prod

config = pulumi.Config()

requirements = config.require("requirements")
job_script = config.require("job-script")
rds_ca_bundle = config.require("rds-ca-bundle")
lz_bucket = config.require("lz-bucket")
source_bucket = config.require("source-bucket")
source_prefix = config.require("source-prefix")
aurora_cluster_id = config.require("aurora-cluster")
aurora_database = config.get("aurora-database") or "orcanexus"
aurora_user = config.get("aurora-user") or "dbuser_rw"
glue_subnet_name = config.require("glue-subnet")
mart_tables = config.require("mart-tables")
shared_infra_path = config.require("shared-infra")
# Default is 18:00 UTC = 04:00 AEST / 05:00 AEDT. Set it after the Redshift UNLOAD.
schedule = config.get("schedule") or "cron(0 18 * * ? *)"
trigger_enabled = config.get_bool("trigger-enabled")

# `make wheels` downloads the wheels pinned in the requirements into this directory.
wheel_dir = "build/wheels"

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
if not aurora_cluster.iam_database_authentication_enabled:
    raise ValueError(
        f"Aurora {aurora_cluster_id} has IAM database authentication off; the job logs in with it"
    )

# Managed by the university: a private subnet of UomPrimaryVpc, the Aurora cluster's VPC.
glue_subnet = aws.ec2.get_subnet(tags={"Name": glue_subnet_name})

job_name = "spreadsheet-mart-tables"

# --- IAM: this module's access, attached to the shared Glue role ---

# The shared role and its common permissions belong to shared-infra. This job's own access
# is an extra inline policy on that role, owned by this stack, so it is deployed and destroyed
# with the job. The role name is the last part of the shared role ARN.
shared_glue_role_name = shared_glue_role_arn.apply(lambda arn: arn.split("/")[-1])

# The login of the database user, which orcahouse infra/aurora/dba/04_rw.sql creates with
# rds_iam. The ARN names the cluster by its resource ID, which outlives a rename.
_, partition, _, region, account = aurora_cluster.arn.split(":")[:5]
aurora_dbuser_arn = (
    f"arn:{partition}:rds-db:{region}:{account}:dbuser:"
    f"{aurora_cluster.cluster_resource_id}/{aurora_user}"
)

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
                # IAM database authentication: an auth token for this database user only
                {
                    "Sid": "AuroraIamAuth",
                    "Effect": "Allow",
                    "Action": ["rds-db:connect"],
                    "Resource": [aurora_dbuser_arn],
                },
            ],
        }
    ),
)

pulumi.export("mart_import_policy_name", mart_import_policy.name)

# --- Network: the job runs in the Aurora cluster's VPC ---

# Glue attaches the job's network interfaces to the subnet with this security group. The
# Aurora security group already allows the whole VPC, so it needs no change.
glue_security_group = aws.ec2.SecurityGroup(
    "glue-security-group",
    name=f"{stack_prefix}-{stack_stage}-{job_name}-sg",
    description=f"Glue job {job_name}: its workers, AWS APIs and Aurora",
    vpc_id=glue_subnet.vpc_id,
)

# Glue requires a self-referencing rule for all TCP ports, between the job's workers.
glue_self_ingress = aws.vpc.SecurityGroupIngressRule(
    "glue-self-ingress",
    security_group_id=glue_security_group.id,
    referenced_security_group_id=glue_security_group.id,
    ip_protocol="tcp",
    from_port=0,
    to_port=65535,
    description="Glue workers",
)

glue_self_egress = aws.vpc.SecurityGroupEgressRule(
    "glue-self-egress",
    security_group_id=glue_security_group.id,
    referenced_security_group_id=glue_security_group.id,
    ip_protocol="tcp",
    from_port=0,
    to_port=65535,
    description="Glue workers",
)

# HTTPS to AWS: S3 through its gateway endpoint, Glue through its interface endpoint, and the
# other APIs (CloudWatch Logs) through the university transit gateway.
glue_https_egress = aws.vpc.SecurityGroupEgressRule(
    "glue-https-egress",
    security_group_id=glue_security_group.id,
    cidr_ipv4="0.0.0.0/0",
    ip_protocol="tcp",
    from_port=443,
    to_port=443,
    description="AWS APIs",
)

glue_aurora_egress = [
    aws.vpc.SecurityGroupEgressRule(
        f"glue-aurora-egress-{aurora_security_group_id}",
        security_group_id=glue_security_group.id,
        referenced_security_group_id=aurora_security_group_id,
        ip_protocol="tcp",
        from_port=aurora_cluster.port,
        to_port=aurora_cluster.port,
        description=f"Aurora {aurora_cluster_id}",
    )
    for aurora_security_group_id in aurora_cluster.vpc_security_group_ids
]

glue_connection = aws.glue.Connection(
    "glue-connection",
    name=f"{stack_prefix}-{stack_stage}-{job_name}-vpc",
    connection_type="NETWORK",
    description=f"Runs the {job_name} job in the VPC of Aurora {aurora_cluster_id}",
    physical_connection_requirements=aws.glue.ConnectionPhysicalConnectionRequirementsArgs(
        availability_zone=glue_subnet.availability_zone,
        subnet_id=glue_subnet.id,
        security_group_id_lists=[glue_security_group.id],
    ),
)

pulumi.export("glue_connection_name", glue_connection.name)
pulumi.export("glue_security_group_id", glue_security_group.id)

# --- S3 Uploads ---

# NOTE: this is intended naming convention, i.e. <datasource>__<suffix_meaningful_naming_convention>
# The load targets are the Aurora mart tables; the base name keys the job files and artifacts.
base_name = "spreadsheet__mart_tables"
s3_mid_path = f"{stack_prefix}/{base_name}/{stack_stage}"


def file_md5(path: str) -> str:
    with open(os.path.abspath(path), "rb") as handle:
        return hashlib.md5(handle.read()).hexdigest()


def pinned_wheels(requirements_path: str) -> list[str]:
    """The downloaded wheel of each pin in the requirements, in the same order."""
    with open(requirements_path) as handle:
        pins = re.findall(r"^([\w.-]+)==(\S+)", handle.read(), flags=re.MULTILINE)

    wheels = []
    for name, version in pins:
        pattern = os.path.join(wheel_dir, f"{name.replace('-', '_')}-{version}-*.whl")
        matches = glob.glob(pattern)
        if len(matches) != 1:
            raise FileNotFoundError(
                f"No wheel {pattern}. Run `make wheels` in the module directory first."
            )
        wheels.extend(matches)
    return wheels


# Upload the job's Python wheels. Glue installs them from S3 only, so a run never needs PyPI.
wheels_s3 = [
    aws.s3.BucketObject(
        f"wheel-{os.path.basename(path)}",
        bucket=landing_zone_bucket.bucket,
        key=f"{s3_mid_path}/wheels/{os.path.basename(path)}",
        source=pulumi.FileAsset(path),
        etag=file_md5(path),
    )
    for path in pinned_wheels(requirements)
]

pulumi.export("wheel_s3_keys", [wheel.key for wheel in wheels_s3])

# Upload the RDS regional CA bundle, which the job verifies Aurora's certificate with
rds_ca_bundle_s3 = aws.s3.BucketObject(
    "rds-ca-bundle",
    bucket=landing_zone_bucket.bucket,
    key=f"{s3_mid_path}/{os.path.basename(rds_ca_bundle)}",
    source=pulumi.FileAsset(rds_ca_bundle),
    etag=file_md5(rds_ca_bundle),
)

pulumi.export("rds_ca_bundle_s3_key", rds_ca_bundle_s3.key)

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
    connections=[glue_connection.name],
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
        rds_ca_bundle_s3.key,
        *[wheel.key for wheel in wheels_s3],
    ).apply(
        lambda args: {
            "--job-language": "python",
            "--python-modules-installer-option": "--no-index",
            "--additional-python-modules": ",".join(
                f"s3://{args[0]}/{key}" for key in args[2:]
            ),
            "--lz_bucket": args[0],
            "--base_name": base_name,
            "--s3_mid_path": s3_mid_path,
            "--source_bucket": source_bucket,
            "--source_prefix": source_prefix,
            "--aurora_host": aurora_cluster.endpoint,
            "--aurora_port": str(aurora_cluster.port),
            "--aurora_user": aurora_user,
            "--aurora_database": aurora_database,
            "--aurora_ca_bundle": f"s3://{args[0]}/{args[1]}",
            "--mart_tables": mart_tables,
            "--allow_empty": "false",
            "--dry_run": "false",
        }
    ),
    # A run must not start before the job's access and network rules exist.
    opts=pulumi.ResourceOptions(
        depends_on=[
            mart_import_policy,
            glue_self_ingress,
            glue_self_egress,
            glue_https_egress,
            *glue_aurora_egress,
        ]
    ),
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
