import json

import pulumi
import pulumi_aws as aws
from orcaglue_shared_lib.tagging import register_global_tags

register_global_tags()

db_name = "orcavault"
stack_prefix = "orcaglue"
stack_stage = pulumi.get_stack()  # dev or prod
# The AWS account both stacks deploy into (115253169271). Shown in the Slack alerts,
# because several accounts post to the same alert channels.
account_name = "umccr-warehouse-prod"

config = pulumi.Config()

lz_bucket = config.require("lz-bucket")
rs_workgroup_name = config.require("rs-workgroup")

# Glue job failure notifications (section 7). Opt-in like the module triggers: the rule
# ships DISABLED until "notify-enabled" is set to true. Leaving "notify-topic-arn" unset
# creates no notification resources at all; it becomes required once enabled.
notify_enabled = config.get_bool("notify-enabled")
enable_notify = notify_enabled if notify_enabled is not None else False
notify_topic_arn = (
    config.require("notify-topic-arn")
    if enable_notify
    else config.get("notify-topic-arn")
)

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

# --- 7. Glue job failure notifications ---

# Glue publishes a "Glue Job State Change" event to this account's default EventBridge
# bus when a job run ends. The rule below matches failed and timed-out runs of this
# stage's jobs and delivers them directly to the Slack-bound SNS topic, which lives in
# another account. 
#
# The topic owner must allow the notify role to publish. The policy statement and the
# rollout order are in shared-infra/README.md#glue-job-failure-notifications.

if notify_topic_arn:
    account_id = aws.get_caller_identity().account_id
    notify_job_prefix = f"{stack_prefix}-{stack_stage}-"
    # Glue emits this event for SUCCEEDED, FAILED, TIMEOUT and STOPPED only. STOPPED is a
    # manual cancel and is left out on purpose; add it here if it should notify too.
    notify_states = ["FAILED", "TIMEOUT"]

    glue_failure_rule = aws.cloudwatch.EventRule(
        f"{stack_prefix}-shared-infra-glue-job-failure-rule-{stack_stage}",
        name=f"{stack_prefix}-shared-infra-glue-job-failure-rule-{stack_stage}",
        description=(
            f"Send {' and '.join(notify_states)} runs of {notify_job_prefix}* "
            "Glue jobs to the Slack SNS topic"
        ),
        event_bus_name="default",
        event_pattern=json.dumps(
            {
                "source": ["aws.glue"],
                "detail-type": ["Glue Job State Change"],
                "detail": {
                    "jobName": [{"prefix": notify_job_prefix}],
                    "state": notify_states,
                },
            }
        ),
        state="ENABLED" if enable_notify else "DISABLED",
    )

    # EventBridge assumes this role to publish to the topic in the other account. The
    # trust is scoped to this rule only, to prevent the confused deputy problem.
    glue_notify_role = aws.iam.Role(
        f"{stack_prefix}-shared-infra-glue-notify-role-{stack_stage}",
        name=f"{stack_prefix}-shared-infra-glue-notify-role-{stack_stage}",
        description=(
            "Assumed by EventBridge to publish failed Glue job runs "
            "to the Slack SNS topic"
        ),
        assume_role_policy=glue_failure_rule.arn.apply(
            lambda rule_arn: json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "events.amazonaws.com"},
                            "Condition": {
                                "StringEquals": {"aws:SourceAccount": account_id},
                                "ArnEquals": {"aws:SourceArn": rule_arn},
                            },
                        }
                    ],
                }
            )
        ),
    )

    glue_notify_role_policy = aws.iam.RolePolicy(
        f"{stack_prefix}-shared-infra-glue-notify-role-inline-policy-{stack_stage}",
        role=glue_notify_role.id,
        policy=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "PublishToSlackTopic",
                        "Effect": "Allow",
                        "Action": "sns:Publish",
                        "Resource": notify_topic_arn,
                    }
                ],
            }
        ),
    )

    # Amazon Q Developer in chat apps (the topic's Slack integration) does not render raw
    # Glue events, so the event is reshaped into its custom notification schema.
    #
    # The Glue error message (detail.message) is left out on purpose. EventBridge does
    # not escape the values it substitutes into the template, so an error containing a
    # double quote or a newline (common in Redshift errors) would produce invalid JSON
    # and Amazon Q would drop the notification. The run link shows the full error.
    slack_notification = {
        "version": "1.0",
        "source": "custom",
        "content": {
            "textType": "client-markdown",
            "title": ":rotating_light: Glue job <state>: <jobName>",
            "description": (
                "*Run ID:* `<jobRunId>`\n"
                f"*Account:* {account_name} (<account>, <region>)\n"
                "*Time:* <time>"
            ),
            "nextSteps": [
                (
                    "Error and logs: https://<region>.console.aws.amazon.com/gluestudio"
                    "/home?region=<region>#/job/<jobName>/run/<jobRunId>"
                ),
            ],
            "keywords": ["OrcaGlue", stack_stage, "<state>"],
        },
    }

    aws.cloudwatch.EventTarget(
        f"{stack_prefix}-shared-infra-glue-job-failure-target-{stack_stage}",
        rule=glue_failure_rule.name,
        event_bus_name="default",
        target_id="slack-sns-topic",
        arn=notify_topic_arn,
        role_arn=glue_notify_role.arn,
        input_transformer=aws.cloudwatch.EventTargetInputTransformerArgs(
            input_paths={
                "account": "$.account",
                "region": "$.region",
                "time": "$.time",
                "jobName": "$.detail.jobName",
                "jobRunId": "$.detail.jobRunId",
                "state": "$.detail.state",
            },
            input_template=json.dumps(slack_notification),
        ),
        opts=pulumi.ResourceOptions(depends_on=[glue_notify_role_policy]),
    )

    pulumi.export("glue_notify_role_arn", glue_notify_role.arn)
    pulumi.export("glue_failure_rule_name", glue_failure_rule.name)
    pulumi.export("glue_failure_rule_state", glue_failure_rule.state)
