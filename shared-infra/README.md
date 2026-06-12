# OrcaGlue Shared Infrastructure

The shared AWS resources for the Glue ETL pipelines.

NOTE: 
Required **admin privilege** for creating IAM roles.
Reach out to Victor to apply the stack changes.
```
export AWS_PROFILE=unimelb-warehouse-prod-admin
```

Login to the Pulumi state bucket.
```
pulumi login s3://pulumi-state-115253169271-ap-southeast-2-an/orcaglue
```

Initialise and configure the stack.
```
pulumi stack init prod --secrets-provider="awskms://alias/pulumi-state-key"

pulumi config set aws:region ap-southeast-2
pulumi config get aws:region
```

Routine deployment. 
```
pulumi stack
pulumi stack ls
pulumi stack select prod

pulumi install

pulumi preview
pulumi up

pulumi stack output
```
