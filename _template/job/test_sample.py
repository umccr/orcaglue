import sys

import pytest
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

import sample


@pytest.fixture(scope="module", autouse=True)
def glue_context():
    session = SparkSession.builder.appName("TestSample").getOrCreate()
    context = GlueContext(session.sparkContext)

    sys.argv.append("--JOB_NAME")
    sys.argv.append("test_count")
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    job = Job(context)
    job.init(args["JOB_NAME"], args)

    yield context

    job.commit()


@pytest.mark.filterwarnings("ignore::FutureWarning", "ignore::UserWarning")
def test_count(glue_context):
    dyf = sample.extract(
        glue_context, "s3://awsglue-datasets/examples/us-legislators/all/persons.json"
    )
    print(f"Total number of rows: {dyf.toDF().count()}")
    assert dyf.toDF().count() == 1961
