# Tech Stack & Conventions

## Runtime target

- **AWS Glue 5.0**, `glueetl` command, worker type `G.1X`, Python 3.
- **Python 3.11** locally (match the Glue runtime; `Python 3.11.15` is the reference). Do NOT over-leverage the newest Python or package versions — stay within what the Glue runtime and Python LTS support. See AWS Glue version support policy before bumping anything.
- Glue/PySpark deps (`aws-glue-libs`, `pyspark==3.5.4`) are sensitive. Check with Victor before touching them, or leave them to him.

## Core libraries

- **PySpark / awsglue** (`GlueContext`, `Job`, `getResolvedOptions`) — job harness.
- **polars** — in-job data cleaning/transformation (not pandas).
- **boto3** — Redshift Data API calls. Already present in the Glue runtime; only a dev/CI dep locally.
- **libumccr** (`libssm`, `libs3`) — SSM secret retrieval and S3 helpers.
- **gspread + google-auth** — Google Sheets sourcing.
- **Pulumi** (`pulumi`, `pulumi-aws`) — infrastructure as code.

Shared runtime deps are pinned in `requirements.txt`. Keep versions pinned.

## ETL job script pattern

Model new jobs on `spreadsheet-google-lims/job/spreadsheet_google_lims.py` and `_template/job/sample.py`.

- Structure logic as free `extract()`, `transform()`, `load()` functions, orchestrated by a `Job` subclass whose `run()` calls them in order then `self.commit()`.
- Entry point:
  ```python
  if __name__ == "__main__":
      session = SparkSession.builder.getOrCreate()
      gc = GlueContext(session.sparkContext)
      MyJob(gc).run()
  ```
- Read job arguments with `getResolvedOptions`. Standard pass-in args: `--lz_bucket`, `--rs_workgroup`, `--rs_role`, plus optional `--base_name`, `--s3_mid_path`, `--JOB_NAME`. Guard optional args by checking `sys.argv` before adding them to the params list.
- Transform rules (TSA discipline): read all columns as strings (`pl.read_csv(..., infer_schema=False)`), normalise placeholders/blanks to null, strip whitespace, drop fully-null rows and unnamed/duplicated columns, then rename columns to `snake_case`. Do not reshape or apply business logic.
- Load = **TRUNCATE then COPY** into `tsa.<base_name>` via the Redshift Data API. Poll `describe_statement` until a terminal state; raise on `FAILED`/`ABORTED`.
- Constants like `SCHEMA_NAME = "tsa"`, `DB_NAME = "orcavault"`, `REGION_NAME = "ap-southeast-2"` live at module top. Region is `ap-southeast-2` everywhere.

## Table DDL

- Provide `job/init.sql` for the target table. It is run **manually** in the Redshift Query Editor (old-school DBA style), not by the job.
- The `transform()` step can regenerate the DDL: comment out the `load()` call and run the job to emit the inferred `CREATE TABLE`.
- All columns are `varchar`. Use `varchar(65535)` for wide/free-text columns.

## Infrastructure (Pulumi)

- Backend is S3: `pulumi login s3://pulumi-state-115253169271-ap-southeast-2-an/orcaglue`. Secrets provider is `awskms://alias/pulumi-state-key`.
- Every Pulumi program calls `register_global_tags()` (from `orcaglue_shared_lib.tagging`) first.
- ETL `__main__.py` uploads the job script and `requirements.txt` to the landing-zone S3 bucket (keyed by `orcaglue/<base_name>/<stage>/`), using an MD5 `etag` to trigger re-upload on change, then creates the `aws.glue.Job` and a `SCHEDULED` `aws.glue.Trigger`.
- Triggers are **opt-in**, gated on a `trigger-enabled` config key defaulting to `false` — never on `stack_stage`. Keep both `enabled` and `start_on_creation` bound to it so creating a stack cannot arm a schedule before a manual run is validated:
  ```python
  schedule = config.get("schedule") or "cron(10 13 * * ? *)"
  trigger_enabled = config.get_bool("trigger-enabled")
  enable_trigger = trigger_enabled if trigger_enabled is not None else False
  ```
- Pulumi state is not proof of the live trigger state: a trigger can be armed out of band with `aws glue start-trigger`, and Pulumi will not detect it without a refresh. Verify with `aws glue get-trigger --name ... --query 'Trigger.State'` (`CREATED` = disabled, `ACTIVATED` = live).
- The Glue job's IAM role comes from a `pulumi.StackReference` to `shared-infra` — never inline a new role per module.
- Deploying requires an authenticated AWS session with admin (`iam:PassRole`). Non-admins ask Victor to apply. AWS auth method is up to you (SSO, profiles, `granted`/`assume`); READMEs only signal *when* you must be authenticated.

## Common commands

Repo root:
- `make install` — pre-commit hooks + dev requirements.
- `make check` — pre-commit on all files, `ruff check`, `ruff format --check`.
- `make scan` / `make deep` — secret scanning (trufflehog, ggshield).
- `make baseline` — regenerate the detect-secrets baseline.

Per ETL module (Pulumi):
- `pulumi stack init dev --secrets-provider="awskms://alias/pulumi-state-key"`
- `pulumi stack select dev` · `pulumi preview` · `pulumi up` · `pulumi stack output`
- `pulumi destroy` · `pulumi stack rm dev`

Glue job runs (see `README_GLUE_JOB.md`):
- `aws glue start-job-run --job-name orcaglue-<stage>-<module>-job`
- `aws glue get-job-run --job-name ... --run-id ...`

Local Glue (optional, ~7GB image; see `README_LOCAL.md`, `local.mk`):
- `make pull` · `make up` · `make ps` · `make glue` (enter container) · `make run` (spark-submit).

## Code style & safety

- Format and lint with **ruff** (`ruff format`, `ruff check`). Keep code ruff-clean.
- `pre-commit` enforces: no commits to `main`/`master`/`release/*`, no AWS credentials or private keys, JSON/YAML validity, and detect-secrets against `.secrets.baseline`.
- Never commit secrets. Google credentials and sheet IDs are read at runtime from SSM Parameter Store (e.g. `/umccr/google/drive/...`), not hard-coded.
- Do not commit directly to `main` — use a branch.
