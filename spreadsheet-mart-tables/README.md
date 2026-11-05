# Spreadsheet Mart Tables (Reverse ETL)

<!-- TOC -->

- [Spreadsheet Mart Tables (Reverse ETL)](#spreadsheet-mart-tables-reverse-etl)
  - [What This Job Does](#what-this-job-does)
    - [Input Contract](#input-contract)
    - [Load and Validation Flow](#load-and-validation-flow)
    - [Blue-Green Swap](#blue-green-swap)
    - [Known Limitations](#known-limitations)
  - [Deployment](#deployment)
    - [Network and Login](#network-and-login)
  - [Aurora Setup](#aurora-setup)
    - [Ownership](#ownership)
    - [Mart DDL](#mart-ddl)
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

- Reads the Redshift UNLOAD manifest of each table it loads: the tables in `--mart_tables`, or
  without it, every table in [job/mart_tables.json](job/mart_tables.json).
- Sets `Content-Encoding: gzip` on each CSV part, by copying it onto itself. The Aurora
  `aws_s3` import only decompresses objects that carry that metadata, and Redshift UNLOAD
  doesn't set it.
- Refreshes each Aurora `mart` table with a [blue-green swap](#blue-green-swap). It loads a
  shadow copy of the table in the `mart_stg` schema, with one `aws_s3.table_import_from_s3`
  call per part, each a PostgreSQL `COPY` from S3. Then it swaps the copy in for the `mart`
  table in one transaction. The OrcaNexus API reads the old table until the swap, and the new
  one after it.
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

1. `SET LOCAL lock_timeout`, which limits how long the swap waits for readers. See
   [Blue-Green Swap](#blue-green-swap).
2. Create the shadow table `mart_stg.<table>` from the table's columns in
   [job/mart_tables.json](job/mart_tables.json). See [Mart DDL](#mart-ddl).
3. Import each part into it with `(format csv, header match)`. `HEADER MATCH` fails the import
   if the CSV header doesn't match the table's columns by name and order, which catches a mart
   that changed without `job/mart_tables.json` being regenerated. For about 2 minutes after
   Aurora resumes from 0 ACU, it doesn't have the `s3Import` role's credentials, and an import
   fails with `XX000 unable to access credentials stored with the database instance`. A
   scheduled run usually finds Aurora paused, so its first import waits this out. The job rolls
   that import back to a savepoint, which keeps the parts already imported, and retries it
   every 10 s for up to 5 minutes. Later imports in the run don't wait.
4. Check that the shadow table's row count equals the sum of the rows reported by the imports.
5. Refuse to replace a non-empty `mart` table with 0 rows, unless the job runs with
   `--allow_empty true`.
6. Build the table's indexes, `ANALYZE` it, and grant `SELECT` on it to the `mart-readers`.
7. Swap: `DROP TABLE mart.<table>`, then `ALTER TABLE mart_stg.<table> SET SCHEMA mart`, then
   `COMMIT`.

Any error rolls the table back, so it keeps its previous rows, and no shadow table is left.
Every table is attempted. The job fails at the end if any table failed, which raises the Glue
job failure alert in Slack through the
[orcahouse notification stack](https://github.com/umccr/orcahouse/tree/main/infra/notification).

The summary at `s3://<lz-bucket>/orcaglue/spreadsheet__mart_tables/<stage>/orcanexus_mart_tables.manifest.json`
records each table's status, parts, bytes, rows before and after, and the manifest's
`LastModified`, which shows how old the unload was. A failed table's error gives the
PostgreSQL SQLSTATE and message. The summary also lists the manifests under the prefix that
the job didn't load (`unlisted_tables`): a new mart that needs adding to `job/mart_tables.json`,
or to the stack's `mart-tables`.

### Blue-Green Swap

Each table is refreshed without downtime for the OrcaNexus API:

- **Nobody else sees the shadow table.** It is in the `mart_stg` schema, which `dbuser_ro`
  can't use, and in a transaction that hasn't committed. A failed load rolls back and leaves
  nothing behind.
- **Readers use the old table during the load.** Until the swap, the job only reads the `mart`
  table, so the API's queries don't wait for the load.
- **The swap is two catalog changes and a commit.** It drops the `mart` table, then moves the
  shadow table into `mart` with its indexes, statistics and grants. Each query sees the old
  table or the new one, never a partly loaded or an empty table. The drop comes first, so the
  shadow table's indexes can keep their names in `mart`.
- **The swap waits briefly, then retries.** The drop needs the table's exclusive lock, so it
  waits for the queries running on the table, and queries that arrive meanwhile queue behind
  it. After 5 seconds, `lock_timeout` ends the wait. The job rolls back to a savepoint taken
  before the swap, waits 10 seconds, and tries again, up to 6 times, without redoing the load.
  If a reader still holds the table, the table fails and keeps its rows.

### Known Limitations

- **A swap can make queries wait**, for up to its lock timeout, while it waits for a
  long-running query on the table. See [Blue-Green Swap](#blue-green-swap). A query or
  transaction held open past the retries, such as an idle transaction in `psql`, fails that
  table's refresh with `55P03`.
- **No views or foreign keys on the mart tables.** The swap won't drop a table that other
  objects depend on, so that table fails with `2BP01`, and keeps its rows.
- **The job sets the grants.** Each swap replaces the table, and the old table's grants go with
  it. The job grants `SELECT` to the roles in `mart-readers`, so don't grant on the tables by
  hand.
- **Each table swaps on its own.** During a run, some tables can be a refresh ahead of others.
- **A column change reaches the API at the swap**, so change the OrcaNexus jOOQ model with it. A
  query prepared before the swap can fail once with `cached plan must not change result type`.
- **A statement has 10 minutes.** The session's `statement_timeout` ends a longer import. The
  job logs the time of each import. If a part gets close, unload with `MAXFILESIZE` to make the
  parts smaller.
- **Empty strings become NULL** while the unload writes NULL as an empty field. See the input
  contract.

## Deployment

Follow **[README_DEPLOY.md](../README_DEPLOY.md)** for prerequisites and the Pulumi flow.
This module differs from the TSA modules: its target is Aurora, not Redshift, and it runs in
the Aurora cluster's VPC.

The prerequisite, once per stage, is **the `dbuser_rw` login**, from the orcahouse DBA script
`infra/aurora/dba/04_rw.sql`. It logs in with IAM database authentication (`rds_iam`), so it
has no password or secret. The script also creates the `mart` and `mart_stg` schemas if they
don't exist, and lets `dbuser_rw` create tables in both. The job creates the `mart` tables at
its first run. See [Aurora Setup](#aurora-setup).

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
| The wheels, the RDS CA bundle and the DDL file | Uploaded next to the job script in the landing zone |

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
| `mart-ddl` | `job/mart_tables.json` | The table DDL, which the job creates each shadow table from. See [Mart DDL](#mart-ddl) |
| `source-bucket` | `orcavault-dev-warehouse-115253169271-ap-southeast-2-an` | The UNLOAD target bucket |
| `source-prefix` | `v1/csv/mart/` | The prefix holding one folder per mart table |
| `aurora-cluster` | `orcanexus-dev` | The Aurora cluster; the stack looks up its endpoint, port, resource ID and security group |
| `aurora-database` | `orcanexus` | The Aurora database |
| `aurora-user` | `dbuser_rw` | The database user, created with `rds_iam` by `04_rw.sql` |
| `glue-subnet` | `UomPrimaryPrivateSubnetA` | The `Name` tag of the private subnet for the Glue connection, in the Aurora writer's AZ |
| `mart-tables` | Not set | Optional. The tables to load, each in `job/mart_tables.json`. Without it, the job loads every table in that file. Prod lists 7 of them; see [Mart DDL](#mart-ddl) |
| `mart-readers` | `dbuser_ro` | The roles granted `SELECT` on each new table |
| `trigger-enabled` | `"false"` | The schedule stays off until the UNLOAD is scheduled and a manual run is validated |
| `schedule` | `cron(0 18 * * ? *)` | 04:00 AEST / 05:00 AEDT. Set it to run after the UNLOAD |

Deploy the `prod` stack only once Aurora `orcanexus-prod` exists and `04_rw.sql` has run on it.
Before its first deploy, check that `glue-subnet` is in the Aurora writer's AZ. Before its first
run, check that the prod UNLOAD has written a manifest for each table in its `mart-tables`.

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
  uploaded wheels with `--no-index`, so a run never reaches PyPI. It installs them into a new
  venv that can't see the runtime's own packages, so the file lists every dependency, including
  `python-dateutil` and `six`, at the Glue 5.0 runtime's versions. `make wheels` installs them
  the same way, so a missing dependency fails there rather than in Glue.

## Aurora Setup

The job creates and replaces the `mart` tables itself, so the only DBA step is
`04_rw.sql`. See [Deployment](#deployment).

### Ownership

`dbuser_rw` creates the `mart` tables, so it owns them, which lets it drop and replace them.
Besides its login (`rds_iam` and `CONNECT`) and `EXECUTE` on both `aws_s3.table_import_from_s3`
overloads (the one the job calls is a wrapper that calls the other), `04_rw.sql` gives it only
`USAGE` and `CREATE` on `mart` and `mart_stg`. The master user `sadmin` can read the tables,
because `rds_superuser` has `pg_read_all_data`, but it can't drop or alter them: it isn't a
member of `dbuser_rw`. To change a table by hand, log in as `dbuser_rw` with an IAM auth token.

A `mart` table that the job didn't create, such as one made by hand as `sadmin`, fails its
refresh with `42501 must be owner of table`. A foreign table fails with `42809`. Drop it as
`sadmin`, with `DROP FOREIGN TABLE` for a foreign table, then start the job, which creates it.
The API has no table until the job's run commits it.

### Mart DDL

[job/mart_tables.json](job/mart_tables.json) holds each table's columns, with the exact
Redshift column types and order, and its indexes. The job creates every shadow table from it,
so a change reaches Aurora at the first run after `pulumi up`, with no DBA step.

- **Columns** are generated from the Redshift mart columns, so don't edit them by hand.
  Regenerate them after a mart changes, from the module root on the host, then change the
  OrcaNexus jOOQ model to match. `make ddl` queries the Redshift dev workgroup. Dev and prod
  share account `115253169271`, so the session is that account's admin profile:

  ```bash
  export AWS_PROFILE=unimelb-warehouse-prod-admin
  make ddl
  git diff job/mart_tables.json
  ```

- **Tables** are the ones already in the file, which `make ddl` regenerates. To add or drop a
  table, name every table: `make ddl mart_tables=bam,fastq,…`.

- **Indexes** are edited by hand, as lists of column names, and `make ddl` keeps them. The job
  names each `<table>_<columns>_idx`, and builds it after the load. Add indexes for the
  OrcaNexus API's filters and sort orders. `lims` has one on `sequencing_run_id`, the column
  the API pages it by.

The file is generated from Redshift dev, whose `mart` has 10 tables. The prod stack's
`mart-tables` lists 7 of them. On 2026-10-27, the prod Redshift `mart` had those 7, with the
same columns as dev's, but not the 3 `em_library_*` marts, which OrcaVault added on 2026-10-23.
Check prod again before its first deploy. If it has all 10, remove `mart-tables` from
`Pulumi.prod.yaml`, and the job loads every table in the file. The job creates only the tables
it loads. If prod's columns ever differ from dev's, `HEADER MATCH` fails that table, and it
keeps its rows.

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
part of the stack. The Aurora tables, the `mart_stg` schema and the `dbuser_rw` login are not.
To retire the module, drop the tables as `dbuser_rw`, their owner, then the login and the schema
as `sadmin`.
