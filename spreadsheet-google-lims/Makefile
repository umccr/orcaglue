install:
	@(cd .. && make install)

# ---

# targets inside Docker container

include ../local.mk

cwd:
	cd /home/hadoop/workspace/spreadsheet-google-lims

run: debug cwd
	spark-submit \
		job/spreadsheet_google_lims.py \
		--lz_bucket "orcahouse-dev-landing-zone-115253169271-ap-southeast-2-an" \
		--rs_workgroup "orcahouse-dev" \
		--rs_role "arn:aws:iam::115253169271:role/dev-redshift-namespace-role"
