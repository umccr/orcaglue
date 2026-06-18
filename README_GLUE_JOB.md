# AWS Glue Job Run

* You use AWS Glue Console UI to run a Glue job.
* You can use the AWS CLI to run a Glue job.

```
aws glue list-jobs
{
    "JobNames": [
        "orcaglue-dev-spreadsheet-library-tracking-metadata-job"
    ]
}
```

```
aws glue start-job-run --job-name orcaglue-dev-spreadsheet-library-tracking-metadata-job
{
    "JobRunId": "jr_1cd13010b965e071fee72fa776211224feb6b3e0f2d42f7fe87178485cdeab65"
}
```

```
aws glue get-job-run --job-name orcaglue-dev-spreadsheet-library-tracking-metadata-job --run-id jr_1cd13010b965e071fee72fa776211224feb6b3e0f2d42f7fe87178485cdeab65
{
    "JobRun": {
        "Id": "jr_1cd13010b965e071fee72fa776211224feb6b3e0f2d42f7fe87178485cdeab65",
        "Attempt": 0,
        "JobName": "orcaglue-dev-spreadsheet-library-tracking-metadata-job",
        "JobMode": "SCRIPT",
        "JobRunQueuingEnabled": false,
        "StartedOn": "2026-06-18T16:27:08.401000+10:00",
        "LastModifiedOn": "2026-06-18T16:27:11.787000+10:00",
        "JobRunState": "RUNNING",
        "PredecessorRuns": [],
        "AllocatedCapacity": 2,
        "ExecutionTime": 0,
        "Timeout": 15,
        "MaxCapacity": 2.0,
        "WorkerType": "G.1X",
        "NumberOfWorkers": 2,
        "LogGroupName": "/aws-glue/jobs",
        "GlueVersion": "5.0"
    }
}
```
