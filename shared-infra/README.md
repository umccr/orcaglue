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

Failed and timed-out runs of this stage's Glue jobs (`orcaglue-<stage>-*`) are posted to Slack.
Glue publishes a `Glue Job State Change` event to this account's default EventBridge bus, and a
rule delivers matching runs straight to the stage's Slack-bound SNS topic in another account. No
event bus or code is needed on the receiving side.

```
Glue job run → default event bus (115253169271) → EventBridge rule → stage's SNS topic → Amazon Q → Slack
```

| Stage | Topic (`notify-topic-arn`) | Topic account | Slack channel |
|---|---|---|---|
| `dev` | `arn:aws:sns:ap-southeast-2:843407916570:AwsChatBotTopic-alerts` | umccr-development (`843407916570`) | `alerts-dev` |
| `prod` | `arn:aws:sns:ap-southeast-2:472057503814:AwsChatBotTopic-alerts` | `472057503814` | `alerts-prod` |

Both topic accounts are in a different AWS organisation from `115253169271`. That is fine: the
notify role and the topic policy are all the access needs.

The stack creates these only when `notify-topic-arn` is set. Each stage gets its own rule and role:

* Rule `orcaglue-shared-infra-glue-job-failure-rule-<stage>` on the default bus, matching `FAILED`
  and `TIMEOUT`.
* Role `orcaglue-shared-infra-glue-notify-role-<stage>`, which EventBridge assumes to call
  `sns:Publish` on the topic. Its trust is scoped to the rule.
* A target that reshapes the event into an Amazon Q Developer custom notification: job, state, run
  ID, account name and ID, time and a link to the run. Several accounts post to the same alert
  channels, so the account name (`account_name` in `__main__.py`, `umccr-warehouse-prod`) shows
  where an alert came from.

The Glue error message is deliberately left out of the Slack message. EventBridge does not escape
the values it puts into the message template, so an error containing a double quote or a newline
(common in Redshift errors) would produce invalid JSON, and Amazon Q would drop the notification.
The run link shows the full error.

| Key | Purpose |
|---|---|
| `notify-topic-arn` | SNS topic to publish to. Unset means the stack creates no notification resources |
| `notify-enabled` | Rule state. Defaults to `false`, so the rule is created `DISABLED`. Requires `notify-topic-arn` |

### Rollout

Roll out to `dev` first and test there, then repeat the same steps for `prod`. Within a stage the
order is fixed: the topic policy names that stage's notify role, and the role must exist before
the topic owner can add it.

Set the stage once per shell, `dev` first and `prod` later:
```
STAGE=dev
```

**1. Create the rule and role.** As admin in `umccr-warehouse-prod` (`115253169271`). The rule
ships disabled.
```
cd shared-infra
pulumi stack select $STAGE
pulumi preview
pulumi up
pulumi stack output glue_notify_role_arn
```
The preview should only add four resources: the rule, the role, its inline policy and the target.

**2. Allow the role to publish.** Authenticated to the stage's topic account (see the table above),
merge the stage's statement into the topic's access policy:
[`dev`](policy/AwsChatBotTopic-policy-statement-dev.json) or
[`prod`](policy/AwsChatBotTopic-policy-statement-prod.json). Its principal must match the
`glue_notify_role_arn` output, and its resource must be the stage's topic.

> Merge the statement; do not replace the policy. The existing statements let that account's own
> services, such as other alert rules, publish to the topic.

In the console: SNS → Topics → `AwsChatBotTopic-alerts` → Edit → Access policy. Append the statement to
the `Statement` array and save.

Or with the CLI, from the repository root. The original policy stays in
`/tmp/topic-policy-before-$STAGE.json` for rollback, and re-running replaces the statement rather
than duplicating it.
```
case $STAGE in dev) TOPIC_ARN=arn:aws:sns:ap-southeast-2:843407916570:AwsChatBotTopic-alerts ;; prod) TOPIC_ARN=arn:aws:sns:ap-southeast-2:472057503814:AwsChatBotTopic-alerts ;; esac
aws sns get-topic-attributes --topic-arn "$TOPIC_ARN" --query Attributes.Policy --output text > /tmp/topic-policy-before-$STAGE.json
jq --slurpfile s shared-infra/policy/AwsChatBotTopic-policy-statement-$STAGE.json \
  '.Statement |= ((if type == "array" then . else [.] end) | map(select(.Sid != $s[0].Sid)) + $s)' \
  /tmp/topic-policy-before-$STAGE.json > /tmp/topic-policy-merged.json
aws sns set-topic-attributes --topic-arn "$TOPIC_ARN" --attribute-name Policy \
  --attribute-value file:///tmp/topic-policy-merged.json
```

Check the topic's encryption as well:
```
aws sns get-topic-attributes --topic-arn "$TOPIC_ARN" --query Attributes.KmsMasterKeyId --output text
```
`None` means nothing more is needed. The AWS managed key `alias/aws/sns` blocks publishing from
another account. In that case the topic needs a customer managed key whose key policy allows the
notify role, and the role needs `kms:GenerateDataKey*` and `kms:Decrypt` added in `__main__.py`.

**3. Enable the rule.** Back in `115253169271`:
```
pulumi config set shared-infra:notify-enabled true
pulumi preview
pulumi up
aws events describe-rule --name orcaglue-shared-infra-glue-job-failure-rule-$STAGE --query State
```
The preview should show one update, the rule's state. `ENABLED` means live. `pulumi config set`
edits `Pulumi.$STAGE.yaml`; commit it so the repository matches the live state. Like the triggers,
a rule changed with the CLI is invisible to Pulumi without a refresh, so confirm the live state
with `describe-rule`.

**4. Test with a forced failure.** `spreadsheet-google-lims` has no live schedule in either stage,
so a manual run cannot clash with one. Overriding the workgroup makes the job fail on its first
Redshift Data API call, before `TRUNCATE`. The run still uploads its artefacts to S3, which is
harmless.
```
aws glue start-job-run --job-name orcaglue-$STAGE-spreadsheet-google-lims-job \
  --arguments '{"--rs_workgroup":"does-not-exist"}'
```
A Slack message should arrive within about a minute of the run failing. Check that its link opens
the failed run.

Dev failures post to the same Slack channel as prod. After the dev test, either keep dev enabled
or set its `notify-enabled` back to `false`.

To stop notifications quickly, run
`aws events disable-rule --name orcaglue-shared-infra-glue-job-failure-rule-$STAGE`, then persist
it with `pulumi config set shared-infra:notify-enabled false && pulumi up`.

### Troubleshooting

If no message arrives, check the rule's metrics in CloudWatch (namespace `AWS/Events`, dimension
`RuleName`). They land a few minutes after the run ends, so wait about five minutes before reading
them, or a real match reads as zero. The `date` syntax below is the macOS one.
```
RULE=orcaglue-shared-infra-glue-job-failure-rule-$STAGE
for m in MatchedEvents SuccessfulInvocationAttempts RetryInvocationAttempts FailedInvocations; do
  echo "$m: $(aws cloudwatch get-metric-statistics --namespace AWS/Events --metric-name "$m" \
    --dimensions Name=RuleName,Value="$RULE" --statistics Sum --period 60 \
    --start-time "$(date -u -v-3H +%Y-%m-%dT%H:%M:%SZ)" --end-time "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --query 'sum(Datapoints[].Sum)' --output text)"
done
```

* `MatchedEvents` is zero: the event did not match. Confirm the rule is `ENABLED` and the job name
  starts with `orcaglue-<stage>-`.
* `FailedInvocations` or `RetryInvocationAttempts` is above zero: EventBridge could not publish. A
  permission error is not retried, so it shows as `FailedInvocations` alone. To see which side
  refused it, check CloudTrail in `115253169271`:
  ```
  aws cloudtrail lookup-events --lookup-attributes AttributeKey=EventName,AttributeValue=AssumeRole --max-results 50 --output json \
    | jq -r '.Events[].CloudTrailEvent | fromjson | select((.requestParameters.roleArn // "") | test("glue-notify-role")) | [.eventTime, (.errorCode // "ok")] | @tsv'
  ```
  If EventBridge assumed the notify role (`ok`), the topic refused the publish. Check that the
  target is the topic carrying the statement, that the statement names this stage's role, and the
  topic's encryption. The two accounts are in different organisations, so an organisation policy
  on either side could also block it.
* `SuccessfulInvocationAttempts` is above zero but nothing reached Slack: Amazon Q dropped the
  message. Check the channel configuration's logs in the stage's topic account, in CloudWatch Logs in
  `us-east-1` under `/aws/chatbot/<configuration-name>`. Logging must be enabled on the channel
  configuration.
