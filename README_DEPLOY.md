# Deployment

<!-- TOC -->
* [Deployment](#deployment)
  * [Prerequisites](#prerequisites)
  * [Stack Configuration](#stack-configuration)
  * [Deploy Process](#deploy-process)
    * [Why the order is fixed](#why-the-order-is-fixed)
  * [Dev Deployment](#dev-deployment)
  * [Production Deployment](#production-deployment)
  * [Enable the scheduled trigger](#enable-the-scheduled-trigger)
  * [Failure Notifications](#failure-notifications)
  * [Dry Run](#dry-run)
  * [Verifying a Deployment](#verifying-a-deployment)
  * [Teardown](#teardown)
  * [Troubleshooting](#troubleshooting)
<!-- TOC -->

This is the single reference for deploying any ETL module, to `dev` or to `prod`. Each module
README covers only what is specific to that module — its config values, its target table and
its own quirks.

We use Pulumi to orchestrate deployment. Every module is an independent Pulumi project with a
`dev` and a `prod` stack.

## Prerequisites

**1. Python environment.** Create a virtual environment (any method) and install the dev
toolchain. See [README_DEV.md](README_DEV.md) for the Python version requirement.

First-time setup, from the repository root:
```
conda activate orcaglue
make install
make check
```

Afterwards, just activate it:
```
conda activate orcaglue
```

**2. Authenticated AWS session with admin privilege.**
```
export AWS_PROFILE=unimelb-warehouse-prod-admin
aws sso login
```
_Admin is required because deploying a Glue job needs `iam:PassRole`. Ask Victor to apply the
stack changes if you are not an admin._

**3. Pulumi backend login.**
```
pulumi whoami --verbose --non-interactive
pulumi login s3://pulumi-state-115253169271-ap-southeast-2-an/orcaglue
```

## Stack Configuration

Each module holds a `Pulumi.dev.yaml` and a `Pulumi.prod.yaml`. Config keys are namespaced by
the Pulumi project name, i.e. `<project-name>:<key>`.

| Key | Purpose |
|---|---|
| `shared-infra` | `StackReference` path to the shared stack supplying the Glue role, e.g. `organization/shared-infra/dev` |
| `requirements` | Path to the shared `requirements.txt` uploaded as the job's Python deps |
| `job-script` | Path to the ETL script uploaded to S3 and run by Glue |
| `lz-bucket` | Landing-zone S3 bucket for the job script and generated artefacts |
| `rs-workgroup` | Redshift Serverless workgroup targeted by the Data API |
| `rs-role` | IAM role ARN Redshift assumes for `COPY` from S3 |
| `trigger-enabled` | Opt-in switch for the scheduled trigger. Defaults to `false` |
| `schedule` | Cron (UTC) for the trigger. Defaults to `cron(10 13 * * ? *)` — 23:10 AEST |

Stage values:

| | dev | prod |
|---|---|---|
| `lz-bucket` | `orcahouse-dev-landing-zone-115253169271-ap-southeast-2-an` | `orcahouse-prod-landing-zone-115253169271-ap-southeast-2-an` |
| `rs-workgroup` | `orcahouse-dev` | `orcahouse-prod` |
| `rs-role` | `arn:aws:iam::115253169271:role/dev-redshift-namespace-role` | `arn:aws:iam::115253169271:role/prod-redshift-namespace-role` |
| `shared-infra` | `organization/shared-infra/dev` | `organization/shared-infra/prod` |
| Glue role | `orcaglue-shared-infra-glue-job-role-dev` | `orcaglue-shared-infra-glue-job-role-prod` |

> Check that `shared-infra` points at the matching stage. A prod module pointing at
> `organization/shared-infra/dev` would be handed the **dev** Glue role, which has no access to
> prod resources.

Both `dev` and `prod` live in the same AWS account (`115253169271`) and region
(`ap-southeast-2`). Isolation is by bucket, workgroup, role name and stack stage only — there is
no account boundary between them.

## Deploy Process

The same five steps apply to **every stage**. `dev` and `prod` differ only in the values used and
in how much caution each step warrants — not in the logic.

```
1. shared-infra      2. init.sql        3. refresh grants    4. module stacks     5. validate
   role perms +   →     target       →     make tables    →     Glue jobs,     →    dry run,
   tsa schema           tables              writable             triggers off        then live
```

| Step | What it produces | Run by |
|---|---|---|
| 1. `shared-infra` | Shared Glue execution role, its inline policy (S3, SSM, Redshift Data API), the `tsa` schema, and the initial grants | Pulumi, admin |
| 2. `init.sql` | One target table per module in the `tsa` schema | Redshift Query Editor, **poweruser** |
| 3. Refresh grants | Makes those tables readable and writable by the Glue role | Query Editor or Pulumi |
| 4. Module stacks | Glue job, uploaded job script, and a trigger that ships **disabled** | Pulumi, admin |
| 5. Validate | A dry run, then a live run, then optionally the schedule | AWS CLI or Glue console |

Steps 1–3 are infrastructure groundwork and are done **once per stage**. Steps 4–5 are per module
and are what you repeat on every routine deployment.

### Why the order is fixed

The `tsa` schema and the Glue role grants are owned by the `shared-infra` stack, not by the
modules. That creates three hard dependencies:

* **1 before 2** — every `init.sql` references `orcavault.tsa.<table>`, and the `tsa` schema is
  created in step 1. Running `init.sql` first fails because the schema does not exist.
* **2 before 3** — step 1's `GRANT ... ON ALL TABLES` runs while `tsa` is still empty, so it
  matches nothing. `ALTER DEFAULT PRIVILEGES` only covers tables created by the same database
  user that set it, and step 2 creates them as a different user. The tables are therefore not
  reliably writable until the grants are re-applied *after* they exist.
* **1 before 4** — each module reads `shared_glue_role_arn` from the `shared-infra` stack through
  a `StackReference`, so that output must exist before a module stack can deploy.

Triggers stay disabled until step 5 passes, so nothing runs unattended before a human has seen it
succeed once.

> Skipping step 3 is the most common cause of a first-run failure: the job uploads its artefacts,
> then fails with a permission error on `TRUNCATE` or `COPY`.

## Dev Deployment

Steps 1–3 of the [Deploy Process](#deploy-process) are already in place for `dev`. Routine work is
steps 4 and 5.

Confirm the shared stack output exists:
```
cd shared-infra
pulumi stack select dev
pulumi stack output shared_glue_role_arn
cd ..
```

**Step 4 — deploy the module.** From the module directory, initialise the stack if it has never
been created:
```
pulumi stack init dev --secrets-provider="awskms://alias/pulumi-state-key"
```

Then deploy:
```
pulumi stack select dev
pulumi stack ls
pulumi preview
pulumi up
pulumi stack output
pulumi stack --show-urns
```

**Step 5 — validate.** Run the job. See [Dry Run](#dry-run) to exercise it without writing to
Redshift, and [Verifying a Deployment](#verifying-a-deployment) for the checks.
```
aws glue start-job-run --job-name orcaglue-dev-<module>-job
```

If you added or changed a module's table, do steps 2 and 3 for `dev` as well — run its
`job/init.sql`, then refresh the grants against the dev role
`orcaglue-shared-infra-glue-job-role-dev`.

## Production Deployment

All five steps of the [Deploy Process](#deploy-process), in order. Prod has never been deployed
for a module until you do it, so treat every step as first-time.

**Step 1 — apply `shared-infra` prod.**
```
cd shared-infra
pulumi stack select prod
pulumi preview
pulumi up
pulumi stack output shared_glue_role_arn
```
Review the preview before applying. It should only **add** resources. Stop if it proposes deleting
or replacing the IAM role — that ARN is the identity granted inside Redshift, so it must not
change.

**Step 2 — create the target tables.** Run each module's `job/init.sql` in Redshift Query Editor
against the prod workgroup, as the warehouse **poweruser**.

Each `init.sql` is `DROP TABLE IF EXISTS` followed by `CREATE TABLE`, so it is destructive on
re-run. Run it once per table, and never while a job or a downstream transformation is active.

**Step 3 — refresh the Glue role grants.** Idempotent, so always run it after creating or
recreating a table. See [Refresh Grant Glue Role](shared-infra/README.md#refresh-grant-glue-role).

Confirm before running any job:
```sql
SELECT has_table_privilege('IAMR:orcaglue-shared-infra-glue-job-role-prod',
                           'tsa.<table_name>', 'INSERT');
```
All target tables must return `true`.

**Step 4 — deploy the modules, one at a time.** From each module directory:
```
pulumi stack init prod --secrets-provider="awskms://alias/pulumi-state-key"
pulumi stack select prod
pulumi config
pulumi preview
pulumi up
pulumi stack output
```
The preview should show the `orcaglue-prod-*` Glue job, S3 objects under
`orcaglue/<base_name>/prod/`, a role ARN ending in `-prod`, and the trigger disabled. Do not start
the next module until the current one has passed step 5.

**Step 5 — validate a manual run.** Dry run first, then a live run:
```
aws glue start-job-run --job-name orcaglue-prod-<module>-job --arguments '{"--dry_run":"true"}'
aws glue start-job-run --job-name orcaglue-prod-<module>-job
```
Only then consider [enabling the scheduled trigger](#enable-the-scheduled-trigger).

## Enable the scheduled trigger

Triggers ship **disabled in new stacks** so a deployment can be validated by hand first. The
existing `spreadsheet-library-tracking-metadata` dev stack is an intentional exception because its
trigger is already live. Enable one module at a time, and keep the schedules staggered so the jobs
do not contend on the same Redshift workgroup.

Glue cron triggers run in UTC only; the AEST column is the equivalent Sydney/Melbourne local time
(UTC+10).

| Module | Schedule (UTC) | AEST (UTC+10) |
|---|---|---|
| `spreadsheet-google-lims` | `cron(10 13 * * ? *)` | 23:10 |
| `spreadsheet-library-tracking-metadata` | `cron(25 13 * * ? *)` | 23:25 |
| `spreadsheet-ica-usage-report` | `cron(40 13 * * ? *)` | 23:40 |

```
pulumi config set <project-name>:trigger-enabled true
pulumi preview
pulumi up
aws glue get-trigger --name orcaglue-<stage>-<module>-job-scheduled-trigger --query 'Trigger.State'
```

`CREATED` means disabled, `ACTIVATED` means live.

To stop a schedule quickly without a deployment:
```
aws glue stop-trigger --name orcaglue-<stage>-<module>-job-scheduled-trigger
```
Then persist it with `pulumi config set <project-name>:trigger-enabled false && pulumi up`,
otherwise the next `pulumi up` re-arms it.

> A trigger armed or stopped out of band with the AWS CLI is invisible to `pulumi preview`
> without a refresh, so Pulumi state is not proof of the live state. Always confirm with
> `aws glue get-trigger`.

## Failure Notifications

Failed and timed-out Glue runs post to Slack through an EventBridge rule owned by the
`shared-infra` stack. Each stage has one rule covering every module, including new ones. Both
stages have it enabled: dev posts to `alerts-dev` and prod to `alerts-prod`. Like the triggers,
`notify-enabled` defaults to `false`, so a new stack's rule starts disabled.

See [Glue Job Failure Notifications](shared-infra/README.md#glue-job-failure-notifications) for the
rollout order, the SNS topic policy and troubleshooting.

## Dry Run

Every ETL module supports `--dry_run`. A dry run performs the real extract and transform and
still uploads the generated artefacts to S3, then stops before the Redshift `TRUNCATE` and
`COPY`. That makes it a safe smoke test against a real environment.

Deployed jobs default to `--dry_run=false`. Override it for a single ad-hoc run:
```
aws glue start-job-run \
  --job-name orcaglue-<stage>-<module>-job \
  --arguments '{"--dry_run":"true"}'
```

Locally, inside the Glue container:
```
make run-dry
```

## Verifying a Deployment

After `pulumi up`, confirm the job is wired to the intended stage:
```
aws glue get-job --job-name orcaglue-<stage>-<module>-job \
  --query 'Job.{Role:Role,Args:DefaultArguments,Ver:GlueVersion,Workers:NumberOfWorkers}'
```

Check that `--lz_bucket`, `--rs_workgroup`, `--rs_role` and `--s3_mid_path` all carry the values
for the stage you deployed, and that the role ARN ends with that stage.

After a job run:
```
aws glue get-job-run --job-name orcaglue-<stage>-<module>-job --run-id <id> \
  --query 'JobRun.{S:JobRunState,Secs:ExecutionTime,Err:ErrorMessage}'
```

`JobRunState` should be `SUCCEEDED`. Job logs are in CloudWatch under `/aws-glue/jobs`. Then
confirm the row count on the target table in Redshift.

## Teardown

Destroys only that module's resources — its Glue job, trigger and uploaded S3 objects:
```
pulumi destroy
pulumi stack rm <stage>
```

Do not `pulumi destroy` the `shared-infra` stack while any module stack still references its
output; that would remove the Glue role every module depends on.

## Troubleshooting

1. **Permission denied on `TRUNCATE` or `COPY`.** The Glue role lacks privileges on the target
table. Re-run step 3 of the [Deploy Process](#deploy-process) and verify with
`has_table_privilege`. This is the usual cause after a table has been dropped and recreated.

2. **`COPY` fails on an unknown column.** The table schema is behind the job's expected columns.
Re-run the module's `job/init.sql` (step 2), then refresh the grants again (step 3).

3. **`init.sql` fails saying the schema does not exist.** Step 1 has not been applied for that
stage. See [Why the order is fixed](#why-the-order-is-fixed).

4. **Grants fail with `user "IAMR:orcaglue-shared-infra-glue-job-role-<stage>" does not exist`.**
Seen on a first-time `shared-infra` apply for a new stage (typically prod): `pulumi up` reports
the inline policy and `CREATE SCHEMA tsa` as created, but the `tsa-grants-<stage>` statement
errors. This is *not* a tables problem — a `GRANT USAGE ON SCHEMA` needs no tables; the failure is
that the *grantee* does not exist. Redshift creates an IAM identity's database user
(`IAMR:<role>`) lazily, on that role's **first authentication** to Redshift — not when the IAM
role is created. A brand-new prod Glue role has never connected, so `IAMR:...-prod` is not yet a
known user and the `GRANT` to it fails. (Dev rarely hits this because its role connected long ago,
so the user already exists.) Confirm with an empty `pg_user` match for the role. Fix: create the
user explicitly in Query Editor as the **poweruser**, then re-apply just the failed grants via
Pulumi:
```sql
CREATE USER "IAMR:orcaglue-shared-infra-glue-job-role-prod" WITH PASSWORD DISABLE;
```
```
cd shared-infra
pulumi stack select prod
pulumi stack --show-urns   # copy the tsa-grants-prod URN
pulumi up --replace 'urn:pulumi:prod::shared-infra::aws:redshiftdata/statement:Statement::orcaglue-shared-infra-glue-role-tsa-grants-prod'
```
`WITH PASSWORD DISABLE` matches the "password disabled" behaviour Redshift uses for its own
auto-created IAM users. `create-schema` and the inline policy are already done, so a plain
`pulumi up` shows them `unchanged` — only the grants need `--replace`. **Notice:** this happens
before step 2, so `tsa` still has no tables. `GRANT USAGE ON SCHEMA` and `ALTER DEFAULT
PRIVILEGES` apply fine, but `GRANT ... ON ALL TABLES IN SCHEMA tsa` matches zero tables. You must
still refresh the grants again after step 2 creates the tables (step 3). Net order: create user →
replace grants → create tables (step 2) → refresh grants (step 3) → verify with
`has_table_privilege`.

5. **Preview wants to replace the IAM role or create a whole new stack.** Stop. You are probably on
the wrong stack, or the project name changed. Check `pulumi stack ls` and `pulumi config`.

6. **Job script changes are not picked up.** The S3 object uses an MD5 `etag`, so a `pulumi up` is
required to re-upload the script after editing it. Note the prod landing-zone bucket is not
versioned, so `git revert` plus `pulumi up` is the only way to roll a script back.

7. **Redshift Query Editor v2 does not show the `orcahouse-prod` workgroup.** Seen when running
step 2 (`init.sql`) or step 3 (grants) against prod for the first time: QEv2's workgroup dropdown
lists dev but not `orcahouse-prod`, so there is nothing to connect to. It is not a permissions or
provisioning problem — the workgroup exists; QEv2 is either pointed at the wrong region or has a
stale connection list that predates the prod workgroup being created. Fix, in order: (a) confirm
the console region selector (top-right) reads **Asia Pacific (Sydney) `ap-southeast-2`**, then
hard-reload QEv2 (`Cmd-Shift-R`) or close and reopen the tab — the dropdown often just has not
refreshed since prod was created; (b) if it still does not appear, explicitly create the
connection in the QEv2 left panel via **+ Create connection** (or the connection dropdown → add)
with **Workgroup:** `orcahouse-prod`, **Authentication:** *Federated user* (uses your current IAM
session, the same way dev connects), **Database:** `orcavault`. **Notice:** everything in this
repo lives in `ap-southeast-2`; a workgroup "missing" from QEv2 is almost always the region
selector on another region, not a real infrastructure gap — verify the region before creating new
connections.

8. **`ERROR: permission denied for schema tsa` when running `init.sql`.** Seen at step 2 in prod:
you are connected to `orcahouse-prod` as the **poweruser** (Federated
`AWSReservedSSO_AWSPowerUserAccess_...`) and `CREATE TABLE` in `tsa` is rejected even though the
schema exists. The cause is schema **ownership**, not a missing grant — `shared-infra` created
`tsa` via the Redshift Data API under whichever identity ran `pulumi up`, so the poweruser is not
the schema owner and cannot create objects in it. In dev this is invisible because `tsa` there is
already owned by the poweruser. Fix: reassign schema ownership to the poweruser in Query Editor
v2, then re-run `init.sql`:
```sql
ALTER SCHEMA tsa OWNER TO "IAMR:AWSReservedSSO_AWSPowerUserAccess_<suffix>";
```
Verify it now matches dev (should return the poweruser as `schema_owner`):
```sql
SELECT n.nspname, u.usename AS schema_owner
FROM pg_namespace n JOIN pg_user u ON n.nspowner = u.usesysid
WHERE n.nspname = 'tsa';
```
**Notice:** the `<suffix>` in the SSO user name is environment-specific — look up your own
poweruser name from the verify query's dev result or from `SELECT current_user;` and substitute it
before running the `ALTER`. This is a one-time ownership fix per stage; it does not replace the step-3 grants
that make tables readable/writable by the Glue role.
