--
-- init.sql
--
-- README:
--   The Aurora PostgreSQL DDL for the spreadsheet-mart-tables load target, the
--   mart schema of the orcanexus database. Generated from the Redshift mart
--   columns (svv_columns) by job/generate_init_sql.py, see `make init-sql`.
--   Do not edit it by hand. Regenerate it when a mart's columns change.
--
--   Run it as the Aurora master user (sadmin) in the orcanexus database, after
--   orcahouse infra/aurora/dba/04_rw.sql has created dbuser_rw, and before the
--   first Glue load.
--
--   It drops and recreates every table, which deletes the loaded rows. Don't
--   run it while the Glue job or the OrcaNexus API is using the tables. The next
--   Glue run reloads them.
--
--   Generated from: Redshift workgroup orcahouse-dev, database orcavault, region ap-southeast-2.
--

CREATE SCHEMA IF NOT EXISTS mart;

DROP TABLE IF EXISTS mart.bam;

CREATE TABLE mart.bam
(
    "portal_run_id"      char(16),
    "cohort_id"          varchar(255),
    "bucket"             varchar(63),
    "key"                varchar(1024),
    "library_id"         varchar(255),
    "filename"           varchar(1024),
    "format"             varchar(255),
    "size"               bigint,
    "storage_class"      varchar(255),
    "e_tag"              varchar(255),
    "last_modified_date" timestamptz
);

DROP TABLE IF EXISTS mart.em_library_aliases;

CREATE TABLE mart.em_library_aliases
(
    "library_id"       varchar(255),
    "alias_library_id" varchar(255),
    "run"              varchar(257),
    "sheet_name"       varchar(257),
    "comments"         varchar(257),
    "record_source"    varchar(255),
    "load_datetime"    timestamptz
);

DROP TABLE IF EXISTS mart.em_library_dns;

CREATE TABLE mart.em_library_dns
(
    "sheet_name"          varchar(257),
    "run"                 varchar(257),
    "comments"            varchar(257),
    "library_id"          varchar(255),
    "alias_library_id"    varchar(255),
    "internal_subject_id" varchar(255),
    "external_subject_id" varchar(255),
    "sample_id"           varchar(255),
    "external_sample_id"  varchar(255),
    "experiment_id"       varchar(255),
    "project_id"          varchar(255),
    "owner_id"            varchar(255),
    "workflow"            varchar(255),
    "phenotype"           varchar(255),
    "type"                varchar(255),
    "assay"               varchar(255),
    "quality"             varchar(255),
    "source"              varchar(255),
    "truseq_index"        varchar(255),
    "record_source"       varchar(255),
    "load_datetime"       timestamptz
);

DROP TABLE IF EXISTS mart.em_library_services;

CREATE TABLE mart.em_library_services
(
    "library_id"           varchar(255),
    "metadata_manager"     smallint,
    "workflow_manager"     smallint,
    "sequence_run_manager" smallint
);

DROP TABLE IF EXISTS mart.fastq;

CREATE TABLE mart.fastq
(
    "sequencing_run_id"   varchar(255),
    "sequencing_run_date" date,
    "portal_run_id"       char(16),
    "cohort_id"           varchar(255),
    "bucket"              varchar(63),
    "key"                 varchar(1024),
    "library_id"          varchar(255),
    "filename"            varchar(1024),
    "format"              varchar(255),
    "size"                bigint,
    "storage_class"       varchar(255),
    "e_tag"               varchar(255),
    "last_modified_date"  timestamptz
);

DROP TABLE IF EXISTS mart.lims;

CREATE TABLE mart.lims
(
    "sequencing_run_id"   varchar(255),
    "sequencing_run_date" date,
    "library_id"          varchar(255),
    "alias_library_id"    varchar(255),
    "internal_subject_id" varchar(255),
    "external_subject_id" varchar(255),
    "sample_id"           varchar(255),
    "external_sample_id"  varchar(255),
    "experiment_id"       varchar(255),
    "project_id"          varchar(255),
    "owner_id"            varchar(255),
    "workflow"            varchar(255),
    "phenotype"           varchar(255),
    "type"                varchar(255),
    "assay"               varchar(255),
    "quality"             varchar(255),
    "source"              varchar(255),
    "truseq_index"        varchar(255),
    "load_datetime"       timestamptz
);

DROP TABLE IF EXISTS mart.vcf;

CREATE TABLE mart.vcf
(
    "portal_run_id"      char(16),
    "cohort_id"          varchar(255),
    "bucket"             varchar(63),
    "key"                varchar(1024),
    "library_id"         varchar(255),
    "filename"           varchar(1024),
    "format"             varchar(255),
    "size"               bigint,
    "storage_class"      varchar(255),
    "e_tag"              varchar(255),
    "last_modified_date" timestamptz
);

DROP TABLE IF EXISTS mart.workflow;

CREATE TABLE mart.workflow
(
    "portal_run_id"     char(16),
    "library_id"        varchar(255),
    "workflow_name"     varchar(255),
    "workflow_version"  varchar(255),
    "workflow_status"   varchar(255),
    "workflow_start"    timestamptz,
    "workflow_end"      timestamptz,
    "workflow_duration" numeric(12,4),
    "workflow_comment"  varchar(65535)
);

DROP TABLE IF EXISTS mart.workflow_cost_report;

CREATE TABLE mart.workflow_cost_report
(
    "portal_run_id"    char(16),
    "workflow_name"    varchar(255),
    "workflow_version" varchar(255),
    "workflow_status"  varchar(255),
    "total_cost"       numeric(18,2),
    "compute_cost"     numeric(18,2),
    "license_cost"     numeric(18,2),
    "ica_project"      varchar(255),
    "cost_unit"        varchar(255)
);

DROP TABLE IF EXISTS mart.workflow_output;

CREATE TABLE mart.workflow_output
(
    "portal_run_id" char(16),
    "cohort_id"     varchar(255),
    "bucket"        varchar(63),
    "prefix"        varchar(1024),
    "key_count"     bigint
);

-- The sadmin default privileges cover these. The grants make the script work
-- whoever runs it.
GRANT SELECT ON ALL TABLES IN SCHEMA mart TO dbuser_ro;
GRANT SELECT, INSERT, TRUNCATE, MAINTAIN ON ALL TABLES IN SCHEMA mart TO dbuser_rw;

SELECT 'mart tables ready' AS status;
