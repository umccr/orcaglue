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

shared_infra_ref = pulumi.StackReference(shared_infra_path)
shared_glue_role_arn = shared_infra_ref.get_output("shared_glue_role_arn")

pulumi.export("shared_glue_role_arn", shared_glue_role_arn)

# ---

# Look up pre-existing S3 bucket
landing_zone_bucket = aws.s3.get_bucket(bucket=lz_bucket)

# --- S3 Uploads ---

job_name = "sample-job"  # FIXME <-- update this

# NOTE: this is intended db table naming convention
# i.e. <datasource>__<suffix_meaningful_naming_convention>
# e.g. <spreadsheet>__<some_research_data_collection>
base_name = "demo__sample_data"  # FIXME <-- update this
s3_mid_path = f"{stack_prefix}/{base_name}/{stack_stage}"

# Upload requirements.txt
requirements_s3 = aws.s3.BucketObject(
    "requirements-txt",
    bucket=landing_zone_bucket.bucket,
    key=f"{s3_mid_path}/requirements.txt",
    source=pulumi.FileAsset(requirements),
    etag=pulumi.Output.from_input(
        open(os.path.abspath(requirements), "rb").read()
    ).apply(lambda content: __import__("hashlib").md5(content).hexdigest()),
)

pulumi.export("requirements_s3_key", requirements_s3.key)

# Upload job script
job_script_s3 = aws.s3.BucketObject(
    "job-script",
    bucket=landing_zone_bucket.bucket,
    key=f"{s3_mid_path}/{os.path.basename(job_script)}",
    source=pulumi.FileAsset(job_script),
    etag=pulumi.Output.from_input(open(os.path.abspath(job_script), "rb").read()).apply(
        lambda content: __import__("hashlib").md5(content).hexdigest()
    ),
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
        }
    ),
)

pulumi.export("glue_job_name", glue_job.name)

# --- Glue Trigger ---

# Only enable in prod
enable_trigger = stack_stage == "prod"

glue_trigger = aws.glue.Trigger(
    "glue-trigger",
    name=pulumi.Output.from_input(glue_job.name).apply(
        lambda name: f"{name}-scheduled-trigger"
    ),
    type="SCHEDULED",
    schedule="cron(10 13 * * ? *)",  # Daily at 13:10 UTC = AEST/AEDT 00:10 AM
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
