# Lab Library Tracking Metadata

<!-- TOC -->
* [Lab Library Tracking Metadata](#lab-library-tracking-metadata)
  * [Motivation](#motivation)
  * [Deployment](#deployment)
  * [Glue Job Run](#glue-job-run)
  * [Local Development](#local-development)
  * [History](#history)
<!-- TOC -->

## Motivation

There are multiple stages of processing the spreadsheet data. This ETL script is the first stage.

At this stage, the key focus is to extract the metadata from the spreadsheet, clean them up, DO NOT RE-SHAPE THE DATA or apply business logic. Harmonise the column names. Treat all columns as string data type if doubts.

The ETL output will target towards the TSA (Transient Staging Area) layer of the warehouse (see [Architecture](https://github.com/umccr/orcahouse-doc/tree/main/arch)). The data loading strategy is always "TRUNCATE AND RELOAD".

We do this because --
* A. The spreadsheet data is very noisy (constant tampering of the rows, columns and cell values with unreliable data constraints).
* B. It is a small dataset in terms of data size, volume and growth.
* C. Yet _very complex_ interrelated data records (multiple rows and columns are interrelated, multiple (X)_IDs are competing themselves and each other).

The strategy is not to solve with one-shot of a silver bullet. We are solving the data complexity challenge with multi-stage processing.

## Deployment

We use Pulumi to orchestrate the deployment of the ETL. Do like so.

Need authenticated AWS session.
```
export AWS_PROFILE=unimelb-warehouse-prod-admin
aws sso login
```
_Required Admin privilege as it needs `iam:PassRole` permission. Ask Victor to apply the stack changes if you are not an admin._

Login to Pulumi backend.
```
pulumi login s3://pulumi-state-115253169271-ap-southeast-2-an/orcaglue
```

Deploy the ETL.
```
pulumi stack
pulumi stack --show-urns
pulumi stack ls
pulumi stack select dev
pulumi preview
pulumi up
pulumi stack output
```

## Glue Job Run

See [README_GLUE_JOB.md](../README_GLUE_JOB.md)

## Local Development

Read [README_LOCAL.md](../README_LOCAL.md)

And do like so.

Authenticate AWS session.
```
export AWS_PROFILE=unimelb-warehouse-prod-poweruser
aws sso login
```

Using `granted` CLI to export AWS env vars.
```
assume
env | grep AWS
```

Change the directory to the module root.
```
cd spreadsheet-library-tracking-metadata
```

Bring up the local glue stack.
```
make up
make ps
```

Enter into the glue instance.
```
make glue
```

Go to the module root inside the container as well.
```
cd workspace/spreadsheet-library-tracking-metadata/
```

Run the diagnostics target to check your AWS credentials.
```
make debug
```

Run the ETL script.
```
make run
```

_Optionally you may temporarily comment out the `load()` function inside `GlueLibraryTrackingMetadata.run()` to avoid the data loading._

## History

This is the next iteration of the original ETL setup at https://github.com/umccr/orcahouse/tree/8808669/infra/glue.
You may have a look at the commit history to see the evolution.
