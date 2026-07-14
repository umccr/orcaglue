import hashlib
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
rs_workgroup = config.require("rs-workgroup")
rs_role = config.require("rs-role")
shared_infra_path = config.require("shared-infra")
source_prefix = config.require("source-prefix")
schedule = config.get("schedule") or "cron(10 13 * * ? *)"
trigger_enabled = config.get_bool("trigger-enabled")

shared_infra_ref = pulumi.StackReference(shared_infra_path)
shared_glue_role_arn = shared_infra_ref.get_output("shared_glue_role_arn")

pulumi.export("shared_glue_role_arn", shared_glue_role_arn)

# ---

# Look up pre-existing S3 bucket
landing_zone_bucket = aws.s3.get_bucket(bucket=lz_bucket)

# --- S3 Uploads ---

job_name = "csv-ica-usage-report"

# NOTE: this is intended db table naming convention
# i.e. <datasource>__<suffix_meaningful_naming_convention>
base_name = "csv__ica_usage_report"
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
    timeout=15,
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
            "--rs_workgroup": rs_workgroup,
            "--rs_role": rs_role,
            "--base_name": base_name,
            "--s3_mid_path": s3_mid_path,
            "--source_prefix": source_prefix,
            "--load_enabled": "true",
        }
    ),
)

pulumi.export("glue_job_name", glue_job.name)
pulumi.export("target_table", f"orcavault.tsa.{base_name}")
pulumi.export("source_prefix", source_prefix)

# --- Glue Trigger ---

# Keep triggers opt-in so prod can be validated manually before scheduling.
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
