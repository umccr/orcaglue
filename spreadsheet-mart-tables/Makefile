install:
	@(cd .. && make install)

# ---

# targets inside Docker container

.PHONY: install cwd run run-dry test init-sql

include ../local.mk

PYTHON ?= python
dry_run ?= false
mart_tables ?= bam,em_library_aliases,em_library_dns,em_library_services,fastq,lims,vcf,workflow,workflow_cost_report,workflow_output

cwd:
	cd /home/hadoop/workspace/spreadsheet-mart-tables

run-dry: dry_run = true
run-dry: run

run: debug cwd
	spark-submit \
		job/spreadsheet_mart_tables.py \
		--lz_bucket "orcahouse-dev-landing-zone-115253169271-ap-southeast-2-an" \
		--s3_mid_path "orcaglue/spreadsheet__mart_tables/dev" \
		--source_bucket "orcavault-dev-warehouse-115253169271-ap-southeast-2-an" \
		--source_prefix "v1/csv/mart/" \
		--aurora_cluster_arn "arn:aws:rds:ap-southeast-2:115253169271:cluster:orcanexus-dev" \
		--aurora_secret_arn "$$(aws secretsmanager describe-secret --secret-id orcanexus/dev/dbuser_rw --query ARN --output text)" \
		--aurora_database "orcanexus" \
		--mart_tables "$(mart_tables)" \
		--dry_run "$(dry_run)"

test:
	$(PYTHON) -m pytest -q job/test_*.py

# Regenerate job/init.sql from the Redshift dev mart columns. Runs on the host, with an
# AWS session that can read the Redshift admin secret.
init-sql:
	$(PYTHON) job/generate_init_sql.py \
		--workgroup orcahouse-dev \
		--secret-arn 'redshift!orcahouse-dev-admin' \
		--tables "$(mart_tables)" \
		--output job/init.sql
