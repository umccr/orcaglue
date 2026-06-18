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

DROP TABLE IF EXISTS orcavault.tsa.spreadsheet__library_tracking_metadata;

-- A combination of extract() and transform() functions from the job script
-- can produce (or infer) the following table schema. Comment out the load()
-- function part in the job script and run it to re-generate the schema DDL
-- definition when applicable.
CREATE TABLE IF NOT EXISTS orcavault.tsa.spreadsheet__library_tracking_metadata
(
    assay	                    varchar,
    comments	                varchar,
    coverage	                varchar,
    experiment_id	            varchar,
    external_sample_id	        varchar,
    external_subject_id	        varchar,
    library_id	                varchar,
    override_cycles	            varchar,
    phenotype	                varchar,
    project_name	            varchar,
    project_owner	            varchar,
    qpcr_id	                    varchar,
    quality	                    varchar,
    "run"	                    varchar,
    sample_id	                varchar,
    sample_name	                varchar,
    samplesheet_sample_id	    varchar,
    "source"	                varchar,
    subject_id	                varchar,
    truseq_index	            varchar,
    "type"	                    varchar,
    workflow	                varchar,
    r_rna	                    varchar,
    study	                    varchar,
    sheet_name	                varchar
);
