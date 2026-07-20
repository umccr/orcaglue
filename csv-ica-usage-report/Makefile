install:
	@(cd .. && make install)

# ---

# targets inside Docker container

.PHONY: install cwd run run-load

include ../local.mk

load_enabled ?= false
source_prefix ?= ica-usage-reports/

cwd:
	cd /home/hadoop/workspace/csv-ica-usage-report

run-load: load_enabled = true
run-load: run

run: debug cwd
	spark-submit \
		job/csv_ica_usage_report.py \
		--lz_bucket "orcahouse-dev-landing-zone-115253169271-ap-southeast-2-an" \
		--rs_workgroup "orcahouse-dev" \
		--rs_role "arn:aws:iam::115253169271:role/dev-redshift-namespace-role" \
		--source_prefix "$(source_prefix)" \
		--load_enabled "$(load_enabled)"
