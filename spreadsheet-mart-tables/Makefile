install:
	@(cd .. && make install)

# ---

# targets inside Docker container

.PHONY: install cwd run-dry test wheels ca-bundle ddl

include ../local.mk

PYTHON ?= python
mart_tables ?= bam,em_library_aliases,em_library_dns,em_library_services,fastq,lims,vcf,workflow,workflow_cost_report,workflow_output

cwd:
	cd /home/hadoop/workspace/spreadsheet-mart-tables

# Only a dry run works locally. The Aurora cluster is private, and only the Glue job reaches it,
# through its VPC connection. Run the live load in Glue; see the README.
run-dry: debug cwd
	spark-submit \
		job/spreadsheet_mart_tables.py \
		--lz_bucket "orcahouse-dev-landing-zone-115253169271-ap-southeast-2-an" \
		--s3_mid_path "orcaglue/spreadsheet__mart_tables/dev" \
		--source_bucket "orcavault-dev-warehouse-115253169271-ap-southeast-2-an" \
		--source_prefix "v1/csv/mart/" \
		--mart_ddl job/mart_tables.json \
		--mart_tables "$(mart_tables)" \
		--dry_run true

test:
	$(PYTHON) -m pytest -q job/test_*.py

# Download the job's wheels for `pulumi up` to upload. Runs on the host, before each deploy.
# pip checks every file against its hash in job/requirements.txt.
wheels:
	rm -rf build/wheels
	$(PYTHON) -m pip download --require-hashes --no-deps --only-binary=:all: \
		--dest build/wheels -r job/requirements.txt

# Refresh the RDS CA bundle after AWS adds a CA. Review the diff before committing it.
ca-bundle:
	curl -fsSL https://truststore.pki.rds.amazonaws.com/ap-southeast-2/ap-southeast-2-bundle.pem \
		-o job/rds-ca-ap-southeast-2-bundle.pem

# Regenerate the columns in job/mart_tables.json from the Redshift dev mart, keeping its
# indexes. Runs on the host, with an AWS session that can read the Redshift admin secret.
ddl:
	$(PYTHON) job/generate_mart_ddl.py \
		--workgroup orcahouse-dev \
		--secret-arn 'redshift!orcahouse-dev-admin' \
		--tables "$(mart_tables)" \
		--output job/mart_tables.json
