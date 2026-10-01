# Project Structure

This is a **monorepo**. Each ETL job is an independent, self-contained Pulumi project in its own subdirectory. Shared code and infrastructure live in dedicated top-level directories.

## Top-level layout

```
orcaglue/
├── _template/                # Copy this to scaffold a new ETL module
├── shared-infra/             # Pulumi stack: shared IAM Glue role + Redshift grants
├── shared-lib/               # Installable Python package: orcaglue_shared_lib
├── spreadsheet-google-lims/  # Example real ETL module
├── requirements.txt          # Runtime deps shared across all Glue jobs
├── requirements-dev.txt      # Dev/CI toolchain (installs requirements.txt + shared-lib)
├── Makefile                  # install / check / scan / baseline targets
├── local.mk                  # Docker-based local Glue runtime targets (included by module Makefiles)
├── compose.yml               # Local AWS Glue 5 container for local dev
└── README*.md                # Layered docs (DEV, LOCAL, GLUE_JOB, REDSHIFT)
```

## Anatomy of an ETL module

Each module directory (e.g. `spreadsheet-google-lims/`) contains:

```
<module>/
├── Pulumi.yaml          # Project name + description (name: python runtime)
├── Pulumi.dev.yaml      # Stack config: shared-infra ref, bucket, workgroup, role, job-script
├── __main__.py          # Pulumi program: uploads script + requirements to S3, creates Glue Job + Trigger
├── Makefile             # includes ../local.mk; adds `run` target (spark-submit inside container)
├── README.md            # Module-specific motivation + deployment notes
└── job/
    ├── <job_name>.py    # The Glue ETL script (extract/transform/load)
    └── init.sql         # DDL for the target TSA table (run manually in Redshift Query Editor)
```

## Shared components

- **`shared-infra/`** — A separate Pulumi stack that provisions the shared IAM role Glue jobs assume (`orcaglue-shared-infra-glue-job-role-<stage>`), its S3/SSM/Redshift Data API permissions, and grants schema privileges on the `tsa` schema. ETL modules consume its output via `pulumi.StackReference` (`organization/shared-infra/<stack>`) to get `shared_glue_role_arn`.
- **`shared-lib/`** — An editable-installed package (`orcaglue_shared_lib`) with cross-cutting Pulumi helpers, e.g. `tagging.register_global_tags()` which applies the standard `umccr-org:*` tag set to every resource.

## Creating a new ETL module

1. `cp -R _template <new-job>` then `cd <new-job>`.
2. Update the `# FIXME` markers: `Pulumi.yaml` name/description, `Pulumi.dev.yaml` project key prefix + `job-script`, and in `__main__.py` the `job_name` and `base_name`.
3. Rename `job/sample.py` and write the ETL logic.
4. Add the target table DDL as `job/init.sql`.
5. Remove the `## REMOVE_ME` scaffolding section from the module README.
6. Init and deploy the Pulumi dev stack (see tech.md).

## Deployment stacks

Stacks are `dev` and `prod`. `pulumi.get_stack()` drives stage-specific naming (the Glue job name and the `orcaglue/<base_name>/<stage>/` S3 prefix).

Scheduled Glue triggers are **opt-in per stack**, never derived from the stage. Each module reads a `trigger-enabled` config key that defaults to `false`, so a freshly created stack never schedules itself before a manual run has been validated. Enable it deliberately with `pulumi config set <project>:trigger-enabled true`. The `schedule` cron is configurable per stack for the same reason; prod staggers the three modules so they do not contend on one Redshift workgroup.

See [Production Deployment](../../README.md#production-deployment) in the root README for the required deploy order, which matters because the `tsa` schema and the Glue role grants belong to `shared-infra`, not to the modules.
