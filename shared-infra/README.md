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
pulumi stack init dev --secrets-provider="awskms://alias/pulumi-state-key"

pulumi config set aws:region ap-southeast-2
pulumi config get aws:region
```

Routine deployment. 
```
pulumi stack select dev
pulumi stack ls
pulumi preview
pulumi up
pulumi stack output
pulumi stack
pulumi stack --show-urns
```

## Refresh Grant Glue Role

Sometimes we need to refresh the TSA grant statement for the Glue execution role.

Do like so.

Select the stack.
```
pulumi stack select dev
```

Grab the grant statement urn from the stack output.
```
pulumi stack --show-urns
```

Refresh the grant statement. _This is idempotent._
```
pulumi up --replace 'urn:pulumi:dev::shared-infra::aws:redshiftdata/statement:Statement::orcaglue-shared-infra-glue-role-tsa-grants-dev'
```

Alternatively, run this directly via Redshift Query Editor.

```sql
GRANT USAGE ON SCHEMA tsa TO "IAMR:orcaglue-shared-infra-glue-job-role-dev";

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
ON ALL TABLES IN SCHEMA tsa
TO "IAMR:orcaglue-shared-infra-glue-job-role-dev";

ALTER DEFAULT PRIVILEGES IN SCHEMA tsa
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
ON TABLES
TO "IAMR:orcaglue-shared-infra-glue-job-role-dev";
```
