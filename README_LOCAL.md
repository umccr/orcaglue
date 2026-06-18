# Local Development

<!-- TOC -->
* [Local Development](#local-development)
  * [Steps](#steps)
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

## Undo AWS

Part the steps use granted `assume` CLI to export AWS environment variables. If you'd like to undo that, you can run the following.

```
source dx.sh
undo-aws
env | grep AWS
```
