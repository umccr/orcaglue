<!-- TOC -->
* [OrcaGlue](#orcaglue)
  * [Development](#development)
  * [New ETL Module](#new-etl-module)
  * [Simple Data Loading](#simple-data-loading)
<!-- TOC -->

# OrcaGlue

OrcaGlue – Frontline ETL for Pipeline Automation Data Warehouse

## Development

A Python shop!

This project is all about writing a Python script that runs on AWS Glue as an ETL job.

* Typical ETL script should be a small and a focused task. 
* Hence, the repo is structured as in [a monorepo](https://www.google.com/search?q=monorepo) manner.
* There are multiple ETL modules organised into subdirectories. 
* Each module has its own README file to follow.

Create a Python virtual environment (any method) and install the dev toolchain [requirements](requirements-dev.txt).

See [README_DEV.md](README_DEV.md) for Python version requirement and more _comprehensive_ setup details.

```
conda activate oncoglue
make install
make check
```

## New ETL Module

Create a new project using the template and go to the project directory.
```
cp -R _template sample-job
cd sample-job
```

Login to Pulumi backend. Need an authenticated AWS session.
```
pulumi whoami --verbose --non-interactive

export AWS_PROFILE=unimelb-warehouse-prod-admin
aws sso login

pulumi login s3://pulumi-state-115253169271-ap-southeast-2-an/orcaglue
```

Initialize the Pulumi dev stack.
```
pulumi stack init dev --secrets-provider="awskms://alias/pulumi-state-key"
```

Deploy the ETL.
```
pulumi stack select dev
pulumi stack ls
pulumi preview
pulumi up
pulumi stack output
pulumi stack
pulumi stack --show-urns
```

Try to run the job via AWS CLI. You may opt to do so via Glue Console UI as well.
```
aws glue list-jobs
aws glue start-job-run --job-name orcaglue-dev-sample-job-job
aws glue get-job-run --job-name orcaglue-dev-sample-job-job --run-id jr_1cd13010b965e071fee72fa776211224feb6b3e0f2d42f7fe87178485cdeab65
```

Tear down the stack.
```
pulumi destroy
pulumi stack rm dev
```

Clean up the project directory.
```
cd ..
rm -rf sample-job
```

## Simple Data Loading

* Sometimes, you might have a use case that requires a simple data loading job without going through the Glue ETL pipeline, yet.
* This may be a use case that you are still exploring before fully committing to the Glue ETL pipeline setup.
* For these kinds of use cases, it is possible to leverage the simplified Redshift data loading via Query Editor.

See [README_REDSHIFT.md](README_REDSHIFT.md) for more details.
