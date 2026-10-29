# Spreadsheet Mart Tables (Reverse ETL)

<!-- TOC -->

- [Spreadsheet Mart Tables (Reverse ETL)](#spreadsheet-mart-tables-reverse-etl)
  - [What This Job Does](#what-this-job-does)
    - [Input Contract](#input-contract)
    - [Load and Validation Flow](#load-and-validation-flow)
    - [Known Limitations](#known-limitations)
  - [Deployment](#deployment)
    - [Network and Login](#network-and-login)
  - [Aurora Setup](#aurora-setup)
  - [Glue Job Run](#glue-job-run)
  - [Local Development](#local-development)
  - [Destroy](#destroy)

<!-- TOC -->

## What This Job Does

This module serves the OrcaVault data marts from an operational PostgreSQL for low-latency
reads: the Aurora PostgreSQL cluster `orcanexus-<stage>`, database `orcanexus`, schema `mart`,
the backend of the OrcaNexus Data API. It is a plain copy with no transformation. Mart
transformations belong upstream, in the OrcaVault dbt project.

The job:

- Reads the Redshift UNLOAD manifest of each table in `--mart_tables`.
- Sets `Content-Encoding: gzip` on each CSV part, by copying it onto itself. The Aurora
  `aws_s3` import only decompresses objects that carry that metadata, and Redshift UNLOAD
  doesn't set it.
- Replaces each Aurora `mart` table in one transaction: `TRUNCATE`, then one
  `aws_s3.table_import_from_s3` call per part, which runs a PostgreSQL `COPY` from S3.
- Writes a JSON run summary to the landing zone.

It runs in the Aurora cluster's VPC through a Glue network connection, and logs in to Aurora
as `dbuser_rw` with IAM database authentication over TLS. See
[Network and Login](#network-and-login).

The job doesn't run the UNLOAD. That is a separate step in OrcaHouse, and this job consumes its
output.

### Input Contract

A Redshift UNLOAD of each mart table, in CSV with a header row, gzip compressed, with a
manifest, into the OrcaVault warehouse bucket:

```text
s3://orcavault-<stage>-warehouse-115253169271-ap-southeast-2-an/v1/csv/mart/
└── lims/
    ├── lims_0000_part_00.gz      # one or more parts, one per slice
    ├── lims_0001_part_00.gz
    └── lims_manifest             # lists the parts and their sizes
```

An UNLOAD that produces this layout:

```sql
UNLOAD ('SELECT * FROM mart.lims')
TO 's3://orcavault-dev-warehouse-115253169271-ap-southeast-2-an/v1/csv/mart/lims/lims_'
IAM_ROLE default
FORMAT AS CSV
HEADER
GZIP
MANIFEST
ALLOWOVERWRITE;
```

- **Header.** Every part must have a header row, because each part is imported with
  `HEADER MATCH`.
- **Empty parts.** A manifest entry with a `content_length` of 0 is skipped. UNLOAD can write
  empty parts, and Aurora rejects a 0-byte file.
- **NULL marker.** Parts unloaded without `NULL AS` write NULL as an empty field. To keep empty
  strings distinct from NULL, unload with `NULL AS '\N'` and run this job with
  `--null_marker '\N'`.
- **Bucket.** The parts must be in the source bucket. The Aurora `s3Import` role
  (`orcanexus-<stage>-rds-s3-import-role`) can only read that bucket.

### Load and Validation Flow

The job first waits until Aurora accepts a login. The cluster scales to 0 ACU and pauses when
idle, and a login resumes it, so connection failures are retried for up to 3 minutes. A
refused login fails the job at once.

Then, for each table, in its own session and one transaction:

1. `SET LOCAL lock_timeout = '30s'`, so a long-running reader can't hold the load forever.
2. Count the current rows, then `TRUNCATE` the table.
3. Import each part with `(format csv, header match)`. `HEADER MATCH` fails the import if the
   CSV header doesn't match the table's columns by name and order, which catches a mart that
   changed without `job/init.sql` being regenerated.
4. Check that the table's row count equals the sum of the rows reported by the imports.
5. Refuse to replace a non-empty table with 0 rows, unless the job runs with
   `--allow_empty true`.
6. `COMMIT`, then `ANALYZE` the table.

Any error rolls the table back, so it keeps its previous rows. Every table is attempted. The
job fails at the end if any table failed, which raises the Glue job failure alert in Slack
through the
[orcahouse notification stack](https://github.com/umccr/orcahouse/tree/main/infra/notification).

The summary at `s3://<lz-bucket>/orcaglue/spreadsheet__mart_tables/<stage>/orcanexus_mart_tables.manifest.json`
records each table's status, parts, bytes, rows before and after, and the manifest's
`LastModified`, which shows how old the unload was. A failed table's error gives the
PostgreSQL SQLSTATE and message. The summary also lists the manifests under the prefix that
aren't in `--mart_tables` (`unlisted_tables`): a new mart that needs adding.

### Known Limitations

- **Readers wait during a load.** `TRUNCATE` takes an exclusive lock until the commit, so
  OrcaNexus API reads of a table wait while that table loads (seconds per table).
- **A statement has 10 minutes.** The session's `statement_timeout` ends a longer import. The
  job logs the time of each import. If a part gets close, unload with `MAXFILESIZE` to make the
  parts smaller.
- **Empty strings become NULL** while the unload writes NULL as an empty field. See the input
  contract.
- **`em_library_*` marts.** `em_library_aliases`, `em_library_dns` and `em_library_services`
  are in the Redshift dev `mart` schema but aren't models in the OrcaVault repository.

## Deployment

Follow **[README_DEPLOY.md](../README_DEPLOY.md)** for prerequisites and the Pulumi flow.
This module differs from the TSA modules: its target is Aurora, not Redshift, and it runs in
the Aurora cluster's VPC.

Prerequisites, once per stage, in order:

1. **The `dbuser_rw` login**, from the orcahouse DBA script `infra/aurora/dba/04_rw.sql`. It
   logs in with IAM database authentication (`rds_iam`), so it has no password or secret.
2. **The target tables**, from [job/init.sql](job/init.sql). See [Aurora Setup](#aurora-setup).

Before each `pulumi preview` or `pulumi up`, download the job's wheels from the module root
on the host. The stack uploads them, and fails if they are missing:

```bash
make wheels
```

`shared-infra` needs no change. The module stack owns the job's access and network, so they
are deployed and destroyed with the job:

| Resource | Purpose |
|---|---|
| Inline policy `orcaglue-<stage>-spreadsheet-mart-tables-policy` on the shared Glue role | The job's S3 and Aurora access, below |
| Security group `orcaglue-<stage>-spreadsheet-mart-tables-sg` | The job's network rules. See [Network and Login](#network-and-login) |
| Glue connection `orcaglue-<stage>-spreadsheet-mart-tables-vpc` | A `NETWORK` connection in `glue-subnet`, which runs the job in the Aurora VPC |
| The wheels and the RDS CA bundle | Uploaded next to the job script in the landing zone |

| Statement | Grants |
|---|---|
| `MartSourceS3List` | `s3:ListBucket` on the source bucket, for keys under `source-prefix` |
| `MartSourceS3Objects` | `s3:GetObject` and `s3:PutObject` under `source-prefix` (reads the manifests, and sets `Content-Encoding: gzip` on the parts in place) |
| `AuroraIamAuth` | `rds-db:connect` as `dbuser_rw` only, on the cluster's resource ID |

The policy is on the shared role, so every OrcaGlue job could use it. A role of its own would
limit it to this job, but the repository keeps one shared Glue role. `MartSourceS3List` states
the job's need without narrowing it, because the role's `AWSGlueServiceRole` managed policy
already allows `s3:ListBucket` on every bucket. That managed policy also lets Glue create the
job's network interfaces.

Module-specific config, in [Pulumi.dev.yaml](Pulumi.dev.yaml):

| Key | Dev value | Purpose |
|---|---|---|
| `requirements` | `job/requirements.txt` | The job's own pinned wheels, not the shared `requirements.txt`. `make wheels` downloads them |
| `rds-ca-bundle` | `job/rds-ca-ap-southeast-2-bundle.pem` | The CA bundle the job verifies Aurora's certificate with |
| `source-bucket` | `orcavault-dev-warehouse-115253169271-ap-southeast-2-an` | The UNLOAD target bucket |
| `source-prefix` | `v1/csv/mart/` | The prefix holding one folder per mart table |
| `aurora-cluster` | `orcanexus-dev` | The Aurora cluster; the stack looks up its endpoint, port, resource ID and security group |
| `aurora-database` | `orcanexus` | The Aurora database |
| `aurora-user` | `dbuser_rw` | The database user, created with `rds_iam` by `04_rw.sql` |
| `glue-subnet` | `UomPrimaryPrivateSubnetA` | The `Name` tag of the private subnet for the Glue connection, in the Aurora writer's AZ |
| `mart-tables` | `bam,em_library_aliases,…,workflow_output` | The tables to load. Keep in sync with `job/init.sql`. Prod lists only the 7 Centre marts |
| `trigger-enabled` | `"false"` | The schedule stays off until the UNLOAD is scheduled and a manual run is validated |
| `schedule` | `cron(0 18 * * ? *)` | 04:00 AEST / 05:00 AEDT. Set it to run after the UNLOAD |

The `prod` stack config is ready, but don't deploy it until Aurora `orcanexus-prod` exists.

### Network and Login

The cluster is private, in the university-managed VPC `UomPrimaryVpc`. The Glue connection
attaches the job's network interfaces to a private subnet of that VPC, so the job connects to
Aurora directly:

- **Login.** For each session, the job generates an IAM auth token for `dbuser_rw` with the
  Glue role's credentials, and logs in with it as the password. The token is signed locally
  and lasts 15 minutes; a session outlives it.
- **TLS.** The job checks Aurora's certificate and host name against the RDS regional CA bundle
  [job/rds-ca-ap-southeast-2-bundle.pem](job/rds-ca-ap-southeast-2-bundle.pem), as
  `sslmode=verify-full` does. When AWS adds a CA, refresh it with `make ca-bundle` and review
  the diff.
- **Security group.** Its rules:

  | Direction | Port | Peer | Purpose |
  |---|---|---|---|
  | In and out | All TCP | Itself | Glue requires it, for traffic between the job's workers |
  | Out | 443 | Anywhere | AWS APIs: S3 through the gateway endpoint, Glue through its interface endpoint, and CloudWatch Logs through the university transit gateway |
  | Out | 5432 | The Aurora security group | PostgreSQL |

  The Aurora security group already allows the whole VPC, so it needs no change.
- **Python packages.** Glue 5.0 has no PostgreSQL driver. [job/requirements.txt](job/requirements.txt)
  pins the pure Python driver pg8000 and its dependencies, with hashes. Glue installs the
  uploaded wheels with `--no-index`, so a run never reaches PyPI.

## Aurora Setup

[job/init.sql](job/init.sql) creates the 10 target tables in `mart`, with the exact Redshift
column types and order. It is generated from the Redshift mart columns, so don't edit it by
hand. Regenerate it after a mart changes, from the module root on the host:

```bash
export AWS_PROFILE=unimelb-warehouse-prod-admin
make init-sql
git diff job/init.sql
```

Run it as the Aurora master user `sadmin` in the `orcanexus` database. Use the RDS Query
Editor with the cluster's master secret, or `psql` through the OrcaNexus bastion tunnel. Run it
after `04_rw.sql`, because its grants name `dbuser_rw`.

> **It drops and recreates every table**, which deletes the loaded rows until the next job
> run reloads them. Don't run it while the job or the OrcaNexus API is using the tables. After
> a column change, regenerate the OrcaNexus jOOQ model as well.

`job/init.sql` is generated from Redshift dev, whose `mart` has 10 tables. The prod Redshift
`mart` has only the 7 Centre marts; their columns are identical to dev's (checked 2026-10-27).
For prod, generate and run the DDL for those 7 tables instead:

```bash
python job/generate_init_sql.py --workgroup orcahouse-prod \
  --secret-arn 'redshift!orcahouse-prod-admin' \
  --tables bam,fastq,lims,vcf,workflow,workflow_cost_report,workflow_output \
  --output /tmp/init-prod.sql
```

## Glue Job Run

See [README_GLUE_JOB.md](../README_GLUE_JOB.md).

```bash
aws glue start-job-run --job-name orcaglue-dev-spreadsheet-mart-tables-job
```

A dry run reads the manifests, sets `Content-Encoding` on the parts and writes the summary,
without touching Aurora:

```bash
aws glue start-job-run \
  --job-name orcaglue-dev-spreadsheet-mart-tables-job \
  --arguments '{"--dry_run":"true"}'
```

To load only some tables, override `--mart_tables`:

```bash
aws glue start-job-run \
  --job-name orcaglue-dev-spreadsheet-mart-tables-job \
  --arguments '{"--mart_tables":"lims,fastq"}'
```

The run summary records each table's row count after the load. To check Aurora itself, query
it with the RDS Query Editor as `sadmin`, or with `psql` through the OrcaNexus bastion tunnel
as `dbuser_ro`.

## Local Development

See [Run a Module](../README_LOCAL.md#run-a-module) for the local Glue container workflow.
Only a dry run works locally: the cluster is private, and only the Glue job reaches it, through
its VPC connection. Run the live load in Glue, as above.

```bash
cd spreadsheet-mart-tables
make up
make glue
# inside the container
cd workspace/spreadsheet-mart-tables/
make run-dry
```

The dry run uses the dev resources. Run the unit tests from the module root in the Conda
environment:

```bash
make test
```

## Destroy

See [Teardown](../README_DEPLOY.md#teardown). The security group and the Glue connection are
part of the stack. The Aurora tables and the `dbuser_rw` login are not; drop them with a DBA
script if the module is retired.
