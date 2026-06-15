import pulumi
import pulumi_aws as aws
import json
from orcaglue_shared_lib.tagging import register_global_tags

register_global_tags()

stack_prefix = "orcaglue"
stack_stage = pulumi.get_stack()  # dev or prod

# 1. Define the Shared IAM Assume Role Policy for Glue
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

# 2. Create the Shared IAM Role
shared_glue_role = aws.iam.Role(
    f"{stack_prefix}-shared-infra-glue-job-role-{stack_stage}",
    name=f"{stack_prefix}-shared-infra-glue-job-role-{stack_stage}",
    assume_role_policy=glue_assume_role_policy,
)

# 3. Attach AWS Managed Policy for Glue Service
role_policy_attachment = aws.iam.RolePolicyAttachment(
    f"{stack_prefix}-shared-infra-glue-job-role-policy-attachment-{stack_stage}",
    role=shared_glue_role.name,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
)

# 4. CRITICAL: Export the ARN so other Pulumi projects can read it dynamically
pulumi.export("shared_glue_role_arn", shared_glue_role.arn)
