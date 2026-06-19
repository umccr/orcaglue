# Pulumi Template for AWS Glue ETL Job

> CHANGE_ME: THIS IS THE TEMPLATE DESCRIPTION PLACEHOLDER

A minimal Pulumi template for provisioning AWS Glue ETL Job using Python.

## REMOVE_ME

Create a new project using the template and go to the project directory.
```
cp -R _template sample-job
cd sample-job
```

Login to Pulumi backend.
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

You can remove this section and update the project description above.

---



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
pulumi stack select dev
pulumi stack ls
pulumi preview
pulumi up
pulumi stack output
pulumi stack
pulumi stack --show-urns
```

Tear down the stack.
```
pulumi destroy
pulumi stack rm dev
```

## Glue Job Run

See [README_GLUE_JOB.md](../README_GLUE_JOB.md)

## Local Development

Read [README_LOCAL.md](../README_LOCAL.md)
