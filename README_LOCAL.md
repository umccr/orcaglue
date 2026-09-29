# Local Development

<!-- TOC -->
* [Local Development](#local-development)
  * [Steps](#steps)
  * [Run a Module](#run-a-module)
  * [Undo AWS](#undo-aws)
<!-- TOC -->

The local development setup is optional! You be fine without it. 

Write your ETL job script, leverage a Pulumi stack deployment mechanism to `dev`, `prod` stack switch for a complete remote dev environment experience.

> **NOTE: Big ~7GB docker image pulling is required to run Glue locally.**

Depends on your _pain-tolerance_ level and, if you, however, would like to opt into, you can run Glue locally and test your job script before deploying to remote AWS Glue.

Here is how.

## Steps

Set up your Python environment. See [README_DEV.md](README_DEV.md)

Login to AWS SSO. Use [granted](Brewfile) cli to `assume` to the role.
```
aws sso login
assume
```

Consider the following public JSON line dataset.
```
aws s3 ls s3://awsglue-datasets/examples/us-legislators/all/persons.json
```

Pull the Glue runtime `~7GB` image.
```
make pull
```

Bring it up.
```
make up
```

Check the status.
```
make ps
```

Try the diagnostics targets.
```
make pwd
make ls
make ls dd=/
make ls dd=/home
make ls dd=/home/hadoop/
make ls dd=/home/hadoop/workspace/
```

Check the Spark version.
```
make spark
```

Go inside the Glue container.
```
make glue
```

It is just another Linux environment.
```
pwd
ls -l
ls -l workspace/

python3 -V
pip3 install -r workspace/requirements.txt

aws sts get-caller-identity
aws s3 ls s3://awsglue-datasets/examples/us-legislators/all/persons.json
```

While inside the Glue container, you can run the job like so.
```
cd workspace/_template/job/

spark-submit sample.py

pytest -s
```

## Run a Module

The loop below is the same for every ETL module. Replace `<module>` with the module directory,
e.g. `spreadsheet-google-lims`.

Authenticate the AWS session and use `granted` to export temporary credentials.
```
export AWS_PROFILE=unimelb-warehouse-prod-poweruser
aws sso login
assume
env | grep AWS
```

Change to the module root and bring up the local Glue stack.
```
cd <module>
make up
make ps
```

_If the container was created before you assumed the role, or the credentials were refreshed,
run `make reload` before `make glue`._

Enter the Glue container, then change to the module root inside it as well.
```
make glue
cd workspace/<module>/
```

Check your AWS access, then run the ETL.
```
make debug
make run
```

To extract and transform without touching Redshift, use dry-run mode. It still uploads the
generated CSV and SQL artefacts to S3, then stops before the `TRUNCATE` and `COPY`.
```
make run-dry
```

All local `make run` targets point at **dev** resources. See
[README_DEPLOY.md](README_DEPLOY.md) for the deployed job equivalents.

## Undo AWS

Part the steps use granted `assume` CLI to export AWS environment variables. If you'd like to undo that, you can run the following.

```
source dx.sh
undo-aws
env | grep AWS
```
