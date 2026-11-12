# REDCap Case

<!-- TOC -->

- [REDCap Case](#redcap-case)
  - [Motivation](#motivation)
  - [Configuration](#configuration)
  - [Deployment](#deployment)
  - [Redshift Table Setup](#redshift-table-setup)
  - [Glue Job Run](#glue-job-run)
  - [Local Development](#local-development)
  <!-- TOC -->

## Motivation

This ETL job exports case (request) records from the UMCCR REDCap project and loads them into the
warehouse. It is the first stage of processing this data.

At this stage, the key focus is to extract the records, clean them up, DO NOT RE-SHAPE THE DATA or apply business logic. Column names are already `snake_case` in REDCap. Treat all columns as string data type if doubts. Checkbox columns (`field___1`) and repeating instruments are kept as exported.

The ETL output will target towards the TSA (Transient Staging Area) layer of the warehouse (see [Architecture](https://github.com/umccr/orcahouse-doc/tree/main/arch)). The data loading strategy is always "TRUNCATE AND RELOAD".

## Configuration

|                |                                                                                        |
| -------------- | -------------------------------------------------------------------------------------- |
| Pulumi project | `redcap-case`                                                                          |
| Glue job       | `orcaglue-<stage>-redcap-case-job`                                                     |
| Job script     | [job/redcap_case.py](job/redcap_case.py)                                               |
| Target table   | `orcavault.tsa.redcap__case`                                                           |
| Source         | REDCap API `https://redcap.unimelb.edu.au/api/`, `record` export, flat CSV, raw values |
| Schedule       | `cron(10 13 * * ? *)` — 23:10 AEST (default)                                           |

The REDCap API token is read at runtime from SSM Parameter Store. The parameter is **not**
stage-scoped, so dev and prod read the same REDCap project.

- `/orcabus/case-manager/redcap/redcap-api-token`

The job fails without loading if the export returns no records, so a bad response cannot leave the
table truncated and empty.

Stack config lives in [Pulumi.dev.yaml](Pulumi.dev.yaml). See
[Stack Configuration](../README_DEPLOY.md#stack-configuration) for what each key means. The
`schedule` key is optional and defaults to the cron above; set it per stack to stagger against the
other modules.

## Deployment

Follow **[README_DEPLOY.md](../README_DEPLOY.md)**.

> **Before the first prod deployment:** steps 1-3 of the
> [Deploy Process](../README_DEPLOY.md#deploy-process) must be done for `prod` — apply
> `shared-infra`, run [job/init.sql](job/init.sql), then refresh the Glue role grants. The `tsa`
> schema and the grants are owned by the `shared-infra` stack, not by this module.

The trigger ships disabled (`trigger-enabled: "false"`). Validate a manual run first, then see
[Enable the scheduled trigger](../README_DEPLOY.md#enable-the-scheduled-trigger).

## Redshift Table Setup

Run [job/init.sql](job/init.sql) in Redshift Query Editor before the first load, as the warehouse
**poweruser**.

It is `DROP TABLE IF EXISTS` followed by `CREATE TABLE`, so re-running it deletes any data
currently in `tsa.redcap__case`. All columns are `varchar(65535)`.

The `transform()` step regenerates this DDL from the exported columns. To refresh it, run the job
with `--dry_run true` and copy the `CREATE TABLE` it prints (also uploaded as a `.sql` artefact to
the landing-zone bucket). Update `init.sql` whenever the REDCap project gains or loses fields,
otherwise `COPY` will fail on a column mismatch.

> After dropping and recreating the table, refresh the Glue role grants or the next job run will
> fail with a permission error. See
> [Refresh Grant Glue Role](../shared-infra/README.md#refresh-grant-glue-role).

## Glue Job Run

See [README_GLUE_JOB.md](../README_GLUE_JOB.md) for the general job run reference.

```
aws glue start-job-run --job-name orcaglue-dev-redcap-case-job
```

The deployed job defaults to `--dry_run=false`. To transform and upload artefacts without
touching Redshift:

```
aws glue start-job-run \
  --job-name orcaglue-dev-redcap-case-job \
  --arguments '{"--dry_run":"true"}'
```

## Local Development

See [Run a Module](../README_LOCAL.md#run-a-module) for the local Glue container workflow, using
`redcap-case` as the module.

```
cd redcap-case
make up
make glue
# inside the container
cd workspace/redcap-case/
make debug
make run        # or: make run-dry
```

Unit tests run without the Glue container:

```
make test
```
