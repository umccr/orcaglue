--
-- init.sql
--
-- README:
--   This init.sql script usage is straightforward. Go to Redshift QueryEditor,
--   paste the contents of this file into the query editor, and run the query.
--   Tweak or adjust the query as needed.
--
--   TIP: Instead of drop and recreating the table, you can use the "ALTER TABLE"
--   command to add new columns. However. The whole point of the "tsa" schema is
--   to truncate and reload data strategy. So, there you have it! Choose whichever
--   approach you prefer.
--
--   I intentionally keep this in the old-school DBA (database admin) way.
--   So, "THIS IS THE WAY" from The Mandalorian lore applied here. ~victor
--

DROP TABLE IF EXISTS orcavault.tsa.spreadsheet__google_lims;

-- A combination of extract() and transform() functions from the job script
-- can produce (or infer) the following table schema. Comment out the load()
-- function part in the job script and run it to re-generate the schema DDL
-- definition when applicable.
CREATE TABLE IF NOT EXISTS orcavault.tsa.spreadsheet__google_lims
(
    illumina_id	            varchar,
    "run"                   varchar,
    "timestamp"	            varchar,
    subject_id	            varchar,
    sample_id	            varchar,
    library_id	            varchar,
    external_subject_id	    varchar,
    external_sample_id	    varchar,
    external_library_id	    varchar,
    sample_name	            varchar,
    project_owner	        varchar,
    project_name	        varchar,
    project_custodian	    varchar,
    "type"	                varchar,
    assay	                varchar,
    override_cycles	        varchar,
    phenotype	            varchar,
    "source"                varchar,
    quality	                varchar,
    topup	                varchar,
    secondary_analysis	    varchar,
    workflow	            varchar,
    tags	                varchar(65535),
    fastq	                varchar(65535),
    number_fastqs	        varchar(65535),
    results	                varchar(65535),
    trello	                varchar(65535),
    notes	                varchar(65535),
    todo	                varchar(65535),
    sheet_name	            varchar
);
