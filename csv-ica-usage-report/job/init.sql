--
-- init.sql
--
-- README:
--   Paste this script into Redshift Query Editor and run it before the first
--   csv_ica_usage_report Glue load.
--
--   This migration intentionally creates only the TSA target table. The Glue
--   job refreshes the current TSA snapshot with TRUNCATE AND RELOAD; historical
--   persistence is handled later in the PSA layer.
--
--   This script drops and recreates the TSA table so the deployed schema
--   matches the current Glue output columns.
--

DROP TABLE IF EXISTS orcavault.tsa.csv__ica_usage_report;

CREATE TABLE IF NOT EXISTS orcavault.tsa.csv__ica_usage_report
(
    usage_id                     varchar(65535),
    uc_name                      varchar(65535),
    billable_account_id          varchar(65535),
    account_name                 varchar(65535),
    account_type                 varchar(65535),
    usage_context                varchar(65535),
    usage_context_type           varchar(65535),
    user_name                    varchar(65535),
    product                      varchar(65535),
    usage_type_description       varchar(65535),
    quantity                     varchar(65535),
    usage_unit                   varchar(65535),
    price_per_unit               varchar(65535),
    cost                         varchar(65535),
    cost_unit                    varchar(65535),
    category                     varchar(65535),
    usage_timestamp              varchar(65535),
    region                       varchar(65535),
    metadata                     varchar(65535),
    billing_date                 varchar(65535),
    ica_execution_id             varchar(65535),
    license                      varchar(65535),
    pipeline_uuid                varchar(65535),
    status                       varchar(65535),
    domain                       varchar(65535),
    type                         varchar(65535),
    workflow_name                varchar(65535),
    workflow_version             varchar(65535),
    portal_run_id                varchar(65535),
    ref_format                   varchar(65535),
    reference_raw                varchar(65535),
    ref_uuid                     varchar(65535),
    id_matches_reference         varchar(65535)
);

SELECT
    'csv__ica_usage_report table ready' AS status;
