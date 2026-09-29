# Lab Library Tracking Metadata

<!-- TOC -->
* [Lab Library Tracking Metadata](#lab-library-tracking-metadata)
  * [Motivation](#motivation)
  * [Configuration](#configuration)
  * [Deployment](#deployment)
  * [Scheduled trigger in dev](#scheduled-trigger-in-dev)
  * [Redshift Table Setup](#redshift-table-setup)
  * [Glue Job Run](#glue-job-run)
  * [Local Development](#local-development)
  * [History](#history)
<!-- TOC -->

## Motivation

There are multiple stages of processing the spreadsheet data. This ETL script is the first stage.

At this stage, the key focus is to extract the metadata from the spreadsheet, clean them up, DO NOT RE-SHAPE THE DATA or apply business logic. Harmonise the column names. Treat all columns as string data type if doubts.

The ETL output will target towards the TSA (Transient Staging Area) layer of the warehouse (see [Architecture](https://github.com/umccr/orcahouse-doc/tree/main/arch)). The data loading strategy is always "TRUNCATE AND RELOAD".

We do this because --
* A. The spreadsheet data is very noisy (constant tampering of the rows, columns and cell values with unreliable data constraints).
* B. It is a small dataset in terms of data size, volume and growth.
* C. Yet _very complex_ interrelated data records (multiple rows and columns are interrelated, multiple (X)_IDs are competing themselves and each other).

The strategy is not to solve with one-shot of a silver bullet. We are solving the data complexity challenge with multi-stage processing.

## Configuration

| | |
|---|---|
| Pulumi project | `spreadsheet-library-tracking-metadata` |
| Glue job | `orcaglue-<stage>-spreadsheet-library-tracking-metadata-job` |
| Job script | [job/spreadsheet_library_tracking_metadata.py](job/spreadsheet_library_tracking_metadata.py) |
| Target table | `orcavault.tsa.spreadsheet__library_tracking_metadata` |
| Source worksheets | one per year, `2017` through `2026` |
| Schedule (prod) | `cron(25 13 * * ? *)` — 23:25 AEST |

The job reads one worksheet per year and concatenates them, adding the sheet name as a column. A
new year means adding it to the `SHEETS` list in the job script.

Datasource credentials are read at runtime from SSM Parameter Store. These parameters are **not**
stage-scoped, so dev and prod read the same spreadsheet.

* `/umccr/google/drive/lims_service_account_json`
* `/umccr/google/drive/tracking_sheet_id`

Stack config lives in [Pulumi.dev.yaml](Pulumi.dev.yaml) and [Pulumi.prod.yaml](Pulumi.prod.yaml).
See [Stack Configuration](../README_DEPLOY.md#stack-configuration) for what each key means.

## Deployment

Follow **[README_DEPLOY.md](../README_DEPLOY.md)**.

> **Before the first prod deployment:** steps 1-3 of the
> [Deploy Process](../README_DEPLOY.md#deploy-process) must be done for `prod` — apply
> `shared-infra`, run [job/init.sql](job/init.sql), then refresh the Glue role grants. The `tsa`
> schema and the grants are owned by the `shared-infra` stack, not by this module.

The prod trigger ships disabled. Validate a manual run first, then see
[Enable the scheduled trigger](../README_DEPLOY.md#enable-the-scheduled-trigger).

## Scheduled trigger in dev

Unlike the other ETL modules, this one also runs on a schedule in **dev**, daily at 13:10 UTC
(23:10 AEST). The trigger was originally armed out of band, so `trigger-enabled: "true"` is
declared in [Pulumi.dev.yaml](Pulumi.dev.yaml) to keep the config honest about the live state.

Confirm the actual state in AWS rather than trusting Pulumi state alone, because a trigger armed
with `aws glue start-trigger` is invisible to `pulumi preview` without a refresh:

```
aws glue get-trigger \
  --name orcaglue-dev-spreadsheet-library-tracking-metadata-job-scheduled-trigger \
  --query 'Trigger.State'
```

`CREATED` means disabled, `ACTIVATED` means live.

## Redshift Table Setup

Run [job/init.sql](job/init.sql) in Redshift Query Editor before the first load, as the warehouse
**poweruser**.

It is `DROP TABLE IF EXISTS` followed by `CREATE TABLE`, so re-running it deletes any data
currently in `tsa.spreadsheet__library_tracking_metadata`. All columns are `varchar`.

The `transform()` step regenerates this DDL from the dataframe. To refresh it, run the job with
`--dry_run true` and copy the `CREATE TABLE` it prints.

> After dropping and recreating the table, refresh the Glue role grants or the next job run will
> fail with a permission error. See
> [Refresh Grant Glue Role](../shared-infra/README.md#refresh-grant-glue-role).

## Glue Job Run

See [README_GLUE_JOB.md](../README_GLUE_JOB.md) for the general job run reference.

```
aws glue start-job-run --job-name orcaglue-dev-spreadsheet-library-tracking-metadata-job
```

The deployed job defaults to `--dry_run=false`. To transform and upload artefacts without
touching Redshift:
```
aws glue start-job-run \
  --job-name orcaglue-dev-spreadsheet-library-tracking-metadata-job \
  --arguments '{"--dry_run":"true"}'
```

## Local Development

See [Run a Module](../README_LOCAL.md#run-a-module) for the local Glue container workflow, using
`spreadsheet-library-tracking-metadata` as the module.

```
cd spreadsheet-library-tracking-metadata
make up
make glue
# inside the container
cd workspace/spreadsheet-library-tracking-metadata/
make debug
make run        # or: make run-dry
```

## History

This is the next iteration of the original ETL setup at https://github.com/umccr/orcahouse/tree/8808669/infra/glue.
You may have a look at the commit history to see the evolution.
