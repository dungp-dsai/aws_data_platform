from pathlib import Path
from tempfile import TemporaryDirectory

import boto3
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F

session = boto3.Session(
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

endpoint = "http://host.docker.internal:4566"
s3 = session.client("s3", endpoint_url=endpoint)
glue = session.client("glue", endpoint_url=endpoint)

glue_context = GlueContext(SparkContext.getOrCreate())
spark = glue_context.spark_session
spark.sparkContext.setLogLevel("WARN")

with TemporaryDirectory() as directory:
    input_directory = Path(directory) / "prs.csv"
    output_directory = Path(directory) / "clean"
    s3.download_file(
        "data-platform-raw",
        "etl/prs.csv",
        str(input_directory)
    )

    raw = (
        spark.read
        .option('header', True)
        .option('mode', "FAILFAST")
        .csv(str(input_directory))
    )

    normalized = raw.select(
        F.lower(F.trim("repo").alias("repo")),
        F.expr("try_cast(trim(pr_number) as int)").alias("pr_number"),
        F.lower(F.trim("author_type")).alias("author_type"),
    )

    # Keep rows with a repository, positive PR number, and known author type.
    valid = normalized.filter(
        F.col("repo").isNotNull()
        & (F.col("repo") != "")
        & F.col("pr_number").isNotNull()
        & (F.col("pr_number") > 0)
        & F.col("author_type").isin("human", "agent")
    )

    # Remove identical records after normalization.
    clean = valid.dropDuplicates()

    print("Raw rows:", raw.count())
    print("Valid rows:", valid.count())
    print("Clean unique rows:", clean.count())
    clean.orderBy("pr_number").show()

    # LOAD: one output file is sufficient for this tiny exercise.
    clean.coalesce(1).write.parquet(str(output_directory))

    parquet_file = next(output_directory.glob("part-*.parquet"))

    s3.upload_file(
        str(parquet_file),
        "data-platform-clean",
        "etl/prs/data.parquet",
    )

# register to aws glue catalog so that athena can query it
table_definition = {
    "Name": "prs_clean",
    "TableType": "EXTERNAL_TABLE",
    "Parameters": {"classification": "parquet"},
    "PartitionKeys": [], 
    "StorageDescriptor": {
        "Columns": [
            {"Name": "repo", "Type": "string"},
            {"Name": "pr_number", "Type": "int"},
            {"Name": "author_type", "Type": "string"}
        ],
        "Location": "s3://data-platform-clean/etl/prs/",
        "InputFormat": (
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
        ),
        "OutputFormat": (
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
        ),
        "SerdeInfo": {
            "SerializationLibrary": (
                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
            )
        },
    }
}

try:
    glue.create_table(DatabaseName="research", TableInput=table_definition)
except glue.exceptions.AlreadyExistsException:
    glue.update_table(
        DatabaseName="research",
        TableInput=table_definition,
    )

print("Published research.prs_clean")
spark.stop()
