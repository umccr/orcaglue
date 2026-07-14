--
-- init.sql
--
-- README:
--   Paste this script into Redshift Query Editor and run it before the first
--   csv_ica_usage_report Glue load.
--
--   This migration intentionally creates the TSA target table plus staging and
--   previous-copy tables. The Glue job replaces target data with DELETE/INSERT
--   batches instead of TRUNCATE, because Redshift TRUNCATE commits immediately.
--
--   No DROP statement is included here. If you need to rebuild these tables,
--   take an explicit dependency check and run the DROP manually.
--

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
    billing_date                 varchar(65535)
);

CREATE TABLE IF NOT EXISTS orcavault.tsa.csv__ica_usage_report__staging
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
    billing_date                 varchar(65535)
);

CREATE TABLE IF NOT EXISTS orcavault.tsa.csv__ica_usage_report__previous
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
    billing_date                 varchar(65535)
);

SELECT
    'csv__ica_usage_report tables ready' AS status;
