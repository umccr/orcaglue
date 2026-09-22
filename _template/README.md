# Pulumi Template for AWS Glue ETL Job

> CHANGE_ME: THIS IS THE TEMPLATE DESCRIPTION PLACEHOLDER

A minimal Pulumi template for provisioning an AWS Glue ETL Job using Python.

## REMOVE_ME

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

Deploy by following [README_DEPLOY.md](../README_DEPLOY.md), then run the job.
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

Then fill in the sections below, remove this one, and update the description above.

---

## Configuration

| | |
|---|---|
| Pulumi project | `sample-job` <!-- FIXME --> |
| Glue job | `orcaglue-<stage>-sample-job-job` <!-- FIXME --> |
| Job script | [job/sample.py](job/sample.py) <!-- FIXME --> |
| Target table | `orcavault.tsa.demo__sample_data` <!-- FIXME --> |
| Schedule (prod) | `cron(10 13 * * ? *)` <!-- FIXME stagger against the other modules --> |

Stack config lives in [Pulumi.dev.yaml](Pulumi.dev.yaml). See
[Stack Configuration](../README_DEPLOY.md#stack-configuration) for what each key means.

## Deployment

Follow **[README_DEPLOY.md](../README_DEPLOY.md)**.

> **Before the first prod deployment:** steps 1-3 of the
> [Deploy Process](../README_DEPLOY.md#deploy-process) must be done for `prod` — apply
> `shared-infra`, run `job/init.sql`, then refresh the Glue role grants. The `tsa` schema and the
> grants are owned by the `shared-infra` stack, not by this module.

The trigger ships disabled (`trigger-enabled: "false"`). Validate a manual run first, then see
[Enable the scheduled trigger](../README_DEPLOY.md#enable-the-scheduled-trigger).

## Redshift Table Setup

Run `job/init.sql` in Redshift Query Editor before the first load, as the warehouse **poweruser**.

## Glue Job Run

See [README_GLUE_JOB.md](../README_GLUE_JOB.md).

## Local Development

See [Run a Module](../README_LOCAL.md#run-a-module).
