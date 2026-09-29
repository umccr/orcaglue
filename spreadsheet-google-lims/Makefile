install:
	@(cd .. && make install)

# ---

# targets inside Docker container

.PHONY: install cwd run run-dry

include ../local.mk

dry_run ?= false

cwd:
	cd /home/hadoop/workspace/spreadsheet-google-lims

run-dry: dry_run = true
run-dry: run

run: debug cwd
	spark-submit \
		job/spreadsheet_google_lims.py \
		--lz_bucket "orcahouse-dev-landing-zone-115253169271-ap-southeast-2-an" \
		--rs_workgroup "orcahouse-dev" \
		--rs_role "arn:aws:iam::115253169271:role/dev-redshift-namespace-role" \
		--dry_run "$(dry_run)"
