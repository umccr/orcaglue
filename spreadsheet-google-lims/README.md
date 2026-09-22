# Google LIMS Spreadsheet

<!-- TOC -->
* [Google LIMS Spreadsheet](#google-lims-spreadsheet)
  * [Motivation](#motivation)
  * [Configuration](#configuration)
  * [Deployment](#deployment)
  * [Redshift Table Setup](#redshift-table-setup)
  * [Glue Job Run](#glue-job-run)
  * [Local Development](#local-development)
  * [History](#history)
<!-- TOC -->

## Motivation

There are multiple stages of processing the spreadsheet data. This ETL script is the first stage.

At this stage, the key focus is to extract the metadata from the spreadsheet, clean them up, DO NOT RE-SHAPE THE DATA or apply business logic. Harmonise the column names. Treat all columns as string data type if doubts.

The ETL output will target towards the TSA (Transient Staging Area) layer of the warehouse (see [Architecture](https://github.com/umccr/orcahouse-doc/tree/main/arch)). The data loading strategy is always "TRUNCATE AND RELOAD".

## Configuration

| | |
|---|---|
| Pulumi project | `spreadsheet-google-lims` |
| Glue job | `orcaglue-<stage>-spreadsheet-google-lims-job` |
| Job script | [job/spreadsheet_google_lims.py](job/spreadsheet_google_lims.py) |
| Target table | `orcavault.tsa.spreadsheet__google_lims` |
| Source worksheet | `Sheet1` |
| Schedule (prod) | `cron(10 13 * * ? *)` — 00:10 AEST/AEDT |

Datasource credentials are read at runtime from SSM Parameter Store. These parameters are **not**
stage-scoped, so dev and prod read the same spreadsheet.

* `/umccr/google/drive/lims_service_account_json`
* `/umccr/google/drive/lims_sheet_id`

Stack config lives in [Pulumi.dev.yaml](Pulumi.dev.yaml) and [Pulumi.prod.yaml](Pulumi.prod.yaml).
See [Stack Configuration](../README_DEPLOY.md#stack-configuration) for what each key means.

## Deployment

Follow **[README_DEPLOY.md](../README_DEPLOY.md)**.

> **Before the first prod deployment:** steps 1-3 of the
> [Deploy Process](../README_DEPLOY.md#deploy-process) must be done for `prod` — apply
> `shared-infra`, run [job/init.sql](job/init.sql), then refresh the Glue role grants. The `tsa`
> schema and the grants are owned by the `shared-infra` stack, not by this module.

The trigger ships disabled in both stacks. Validate a manual run first, then see
[Enable the scheduled trigger](../README_DEPLOY.md#enable-the-scheduled-trigger).

## Redshift Table Setup

Run [job/init.sql](job/init.sql) in Redshift Query Editor before the first load, as the warehouse
**poweruser**.

It is `DROP TABLE IF EXISTS` followed by `CREATE TABLE`, so re-running it deletes any data
currently in `tsa.spreadsheet__google_lims`. All columns are `varchar`.

The `transform()` step regenerates this DDL from the dataframe. To refresh it, run the job with
`--dry_run true` and copy the `CREATE TABLE` it prints.

> After dropping and recreating the table, refresh the Glue role grants or the next job run will
> fail with a permission error. See
> [Refresh Grant Glue Role](../shared-infra/README.md#refresh-grant-glue-role).

## Glue Job Run

See [README_GLUE_JOB.md](../README_GLUE_JOB.md) for the general job run reference.

```
aws glue start-job-run --job-name orcaglue-dev-spreadsheet-google-lims-job
```

The deployed job defaults to `--dry_run=false`. To transform and upload artefacts without
touching Redshift:
```
aws glue start-job-run \
  --job-name orcaglue-dev-spreadsheet-google-lims-job \
  --arguments '{"--dry_run":"true"}'
```

## Local Development

See [Run a Module](../README_LOCAL.md#run-a-module) for the local Glue container workflow, using
`spreadsheet-google-lims` as the module.

```
cd spreadsheet-google-lims
make up
make glue
# inside the container
cd workspace/spreadsheet-google-lims/
make debug
make run        # or: make run-dry
```

## History

This is the next iteration of the original ETL setup at https://github.com/umccr/orcahouse/tree/8808669/infra/glue.
You may have a look at the commit history to see the evolution.
