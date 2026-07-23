install:
	@(cd .. && make install)

# ---

# targets inside Docker container

.PHONY: install cwd run run-dry test

include ../local.mk

PYTHON ?= python
dry_run ?= false
source_prefix ?= ica-usage-reports/

cwd:
	cd /home/hadoop/workspace/spreadsheet-ica-usage-report

run-dry: dry_run = true
run-dry: run

run: debug cwd
	spark-submit \
		job/spreadsheet_ica_usage_report.py \
		--lz_bucket "orcahouse-dev-landing-zone-115253169271-ap-southeast-2-an" \
		--rs_workgroup "orcahouse-dev" \
		--rs_role "arn:aws:iam::115253169271:role/dev-redshift-namespace-role" \
		--source_prefix "$(source_prefix)" \
		--dry_run "$(dry_run)"

test:
	$(PYTHON) -m pytest -q job/test_*.py
