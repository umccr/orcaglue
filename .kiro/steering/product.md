# Product

OrcaGlue — Frontline ETL for Pipeline Automation Data Warehouse.

It is part of the UMCCR **orcahouse** data warehouse ecosystem. Reference architecture and background docs live at https://github.com/umccr/orcahouse-doc.

## What it does

OrcaGlue is a Python shop. Each deliverable is a small, focused ETL script that runs on **AWS Glue** as an ETL job. Jobs extract data from a source (spreadsheets, APIs, S3), lightly clean it, and load it into an **Amazon Redshift Serverless** warehouse (`orcavault` database).

## Warehouse target: the TSA layer

- ETL output targets the **TSA (Transient Staging Area)** layer of the warehouse — the `tsa` schema in the `orcavault` database.
- The data loading strategy for TSA is always **TRUNCATE AND RELOAD**.
- At the TSA stage, the focus is to extract and clean, **not** to re-shape data or apply business logic:
  - Do NOT re-shape the data.
  - Do NOT apply business logic.
  - Harmonise (normalise) column names.
  - Treat all columns as string (`varchar`) when in doubt.

## Table naming convention

Tables follow `<datasource>__<meaningful_suffix>` (double underscore separator).

Examples:
- `spreadsheet__google_lims`
- `demo__sample_data`

## Simple data loading (escape hatch)

For exploratory use cases that don't yet warrant a full Glue ETL pipeline, data can be loaded/unloaded directly through Redshift Query Editor v2 using `COPY` / `UNLOAD`. See `README_REDSHIFT.md`. Discuss the arrangement with Victor.
