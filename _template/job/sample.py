import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession


def extract(glue_context, s3_uri: str):
    print("-" * 100)
    print(f"EXTRACT: Extracting data from {s3_uri} and creating a DynamicFrame")
    print("-" * 100)

    dynamic_frame = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [s3_uri], "recurse": True},
        format="json",
    )

    return dynamic_frame


def transform(dyf):
    print("-" * 100)
    print("TRANSFORM: Schema of the DynamicFrame")
    print("-" * 100)

    dyf.printSchema()


def load(spark: SparkSession, dyf):
    print("-" * 100)
    print("LOAD: Printing the first 1 row of the DynamicFrame")
    print("-" * 100)

    print(dyf.toDF().head(1))


class SampleJob(Job):
    def __init__(self, glue_context: GlueContext):
        super().__init__(glue_context)

        self.glue_context: GlueContext = glue_context
        self.spark: SparkSession = glue_context.spark_session

        params = []
        if "--JOB_NAME" in sys.argv:
            params.append("JOB_NAME")

        args = getResolvedOptions(sys.argv, params)

        # Resolves pass-in arguments
        self.s3_uri = args.get(
            "s3_uri", "s3://awsglue-datasets/examples/us-legislators/all/persons.json"
        )

        job_name = args.get("JOB_NAME", "SampleJob")
        self.init(job_name, args)

    def run(self):

        dyf = extract(self.glue_context, self.s3_uri)

        transform(dyf)

        load(self.spark, dyf)

        self.commit()


if __name__ == "__main__":
    session = SparkSession.builder.getOrCreate()
    gc = GlueContext(session.sparkContext)
    SampleJob(gc).run()
