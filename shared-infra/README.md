# OrcaGlue Shared Infrastructure

The shared AWS resources for the Glue ETL pipelines — the Glue execution role, its inline policy,
the `tsa` schema and the role grants on it.

This stack is **step 1 of the [Deploy Process](../README_DEPLOY.md#deploy-process)** and must be
applied before any ETL module stack, because every module reads its `shared_glue_role_arn` output
and the modules do not own the `tsa` schema. The [Refresh Grant Glue Role](#refresh-grant-glue-role)
section below is step 3 of that process.

NOTE: 
Required **admin privilege** for creating IAM roles.
Reach out to Victor to apply the stack changes.
```
export AWS_PROFILE=unimelb-warehouse-prod-admin
```

Login to the Pulumi state bucket.
```
pulumi login s3://pulumi-state-115253169271-ap-southeast-2-an/orcaglue
```

Initialise and configure the stack.
```
pulumi stack init dev --secrets-provider="awskms://alias/pulumi-state-key"

pulumi config set aws:region ap-southeast-2
pulumi config get aws:region
```

Routine deployment. 
```
pulumi stack select dev
pulumi stack ls
pulumi preview
pulumi up
pulumi stack output
pulumi stack
pulumi stack --show-urns
```

## Refresh Grant Glue Role

Sometimes we need to refresh the TSA grant statement for the Glue execution role.

Do like so.

Select the stack.
```
pulumi stack select dev
```

Grab the grant statement urn from the stack output.
```
pulumi stack --show-urns
```

Refresh the grant statement. _This is idempotent._
```
pulumi up --replace 'urn:pulumi:dev::shared-infra::aws:redshiftdata/statement:Statement::orcaglue-shared-infra-glue-role-tsa-grants-dev'
```

Alternatively, run this directly via Redshift Query Editor.

```sql
GRANT USAGE ON SCHEMA tsa TO "IAMR:orcaglue-shared-infra-glue-job-role-dev";

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
ON ALL TABLES IN SCHEMA tsa
TO "IAMR:orcaglue-shared-infra-glue-job-role-dev";

ALTER DEFAULT PRIVILEGES IN SCHEMA tsa
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
ON TABLES
TO "IAMR:orcaglue-shared-infra-glue-job-role-dev";
```

## Glue Job Failure Notifications

Failed and timed-out runs of this stage's Glue jobs (`orcaglue-<stage>-*`) post to Slack. Glue
publishes a `Glue Job State Change` event to the default EventBridge bus, and a rule sends matching
runs straight to the stage's SNS topic in another account, where Amazon Q Developer posts them to
Slack.

```
Glue job run → default event bus (115253169271) → EventBridge rule → stage's SNS topic → Amazon Q → Slack
```

| Stage | Topic (`notify-topic-arn`) | Slack channel |
|---|---|---|
| `dev` | `arn:aws:sns:ap-southeast-2:843407916570:AwsChatBotTopic-alerts` | `alerts-dev` |
| `prod` | `arn:aws:sns:ap-southeast-2:472057503814:AwsChatBotTopic-alerts` | `alerts-prod` |

When `notify-topic-arn` is set, the stack creates a rule matching `FAILED` and `TIMEOUT`
(`orcaglue-shared-infra-glue-job-failure-rule-<stage>`), a role EventBridge assumes to publish to
the topic (`orcaglue-shared-infra-glue-notify-role-<stage>`), and a target that formats the Slack
message. `notify-enabled` sets the rule state and defaults to `false`.

The message shows the job, state, run ID, account (`umccr-warehouse-prod`), time and a link to the
run. It leaves out the Glue error message: EventBridge doesn't escape it, so an error containing a
quote would break the message. The link shows the full error.

The topic accounts are in a different AWS organisation. That's fine: the role and the topic policy
are all the access needs.

### Rollout

Do `dev` first, then `prod`. Within a stage the order matters, because the topic policy names the
stage's role and the role has to exist first.

1. **Deploy.** As admin in `115253169271`, run `pulumi up` on the stage (see above). It adds four
   resources, with the rule disabled.
2. **Allow the role to publish.** In the topic's account, open SNS → Topics →
   `AwsChatBotTopic-alerts` → Edit → Access policy, and add the stage's statement
   ([dev](policy/AwsChatBotTopic-policy-statement-dev.json), [prod](policy/AwsChatBotTopic-policy-statement-prod.json)) to the existing ones. Don't replace the policy. The topic must not be encrypted 
   with the AWS managed `aws/sns` key, which blocks publishing from another account.
3. **Enable.** Run `pulumi config set shared-infra:notify-enabled true` and `pulumi up`, then commit
   `Pulumi.<stage>.yaml`.
4. **Test.** Force a failure, then check the stage's Slack channel:
   ```
   aws glue start-job-run --job-name orcaglue-<stage>-spreadsheet-google-lims-job --arguments '{"--rs_workgroup":"does-not-exist"}'
   ```
   The made-up workgroup makes the job fail before `TRUNCATE`, so no table is touched. The message
   should arrive within a minute or two.

To stop alerts quickly, run `aws events disable-rule --name orcaglue-shared-infra-glue-job-failure-rule-<stage>`.
Then set `notify-enabled` to `false` and run `pulumi up`, or the next deploy turns it back on.

### Troubleshooting

Check the rule's CloudWatch metrics (namespace `AWS/Events`, by `RuleName`). They arrive a few
minutes late, so wait about five minutes after the run.

* **`MatchedEvents` is 0:** the event didn't match. Check the rule is enabled and the job name
  starts with `orcaglue-<stage>-`.
* **`FailedInvocations` is above 0:** the topic refused the publish. Check the target is the topic
  that has the statement, and the statement names this stage's role.
* **`SuccessfulInvocationAttempts` is above 0 but Slack is empty:** Amazon Q dropped the message.
  Check its channel logs in the topic's account.
