<!-- TOC -->
* [OrcaGlue](#orcaglue)
  * [Development](#development)
  * [Deployment](#deployment)
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

## Deployment

See **[README_DEPLOY.md](README_DEPLOY.md)** for the full deployment reference — prerequisites,
stack configuration, the deploy process, dev and prod deployment, scheduled triggers, dry runs
and troubleshooting.

The same five steps apply to every stage, and the order matters because the `tsa` schema and the
Glue role grants are owned by the `shared-infra` stack rather than by the modules.

```
1. shared-infra   →   2. init.sql   →   3. refresh grants   →   4. module stacks   →   5. validate
```

See [Deploy Process](README_DEPLOY.md#deploy-process) for what each step does and why the order is
fixed. Each module README covers only what is specific to that module.

## New ETL Module

Create a new project using the template and go to the project directory.
```
cp -R _template sample-job
cd sample-job
```

Update the `# FIXME` markers in the copied files.

* `Pulumi.yaml` — project `name` and `description`.
* `Pulumi.dev.yaml` — the `<project-name>:` key prefix and `job-script`.
* `__main__.py` — `job_name` and `base_name`.

Rename `job/sample.py`, write the ETL logic, and add the target table DDL as `job/init.sql`.
Then remove the `## REMOVE_ME` scaffolding section from the module README.

Deploy it by following [README_DEPLOY.md](README_DEPLOY.md), then run the job.
```
aws glue list-jobs
aws glue start-job-run --job-name orcaglue-dev-sample-job-job
```

Clean up the throwaway project when you are done experimenting.
```
pulumi destroy
pulumi stack rm dev
cd ..
rm -rf sample-job
```

## Simple Data Loading

* Sometimes, you might have a use case that requires a simple data loading job without going through the Glue ETL pipeline, yet.
* This may be a use case that you are still exploring before fully committing to the Glue ETL pipeline setup.
* For these kinds of use cases, it is possible to leverage the simplified Redshift data loading via Query Editor.

See [README_REDSHIFT.md](README_REDSHIFT.md) for more details.
