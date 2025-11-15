import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

MINIO_ENDPOINT = "http://minio:9000"
# MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

# For Spark 3.3, Scala 2.12 is standard.
DELTA_PACKAGE = "io.delta:delta-core_2.12:2.4.0"
S3A_PACKAGES = "org.apache.hadoop:hadoop-aws:3.4.2,com.amazonaws:aws-java-sdk:1.12.793"


def setup_spark_with_aws():
    builder = (
        SparkSession.builder.appName("DeltaLakeS3Local")
        .config(
            "spark.jars",
            "/Users/esak/.ivy2/jars/aws-java-sdk-bundle-1.12.593.jar,/Users/esak/.ivy2/jars/hadoop-aws-3.3.4.jar,/Users/esak/.ivy2/jars/io.delta_delta-spark_2.12-3.3.0.jar,/Users/esak/.ivy2/jars/postgresql-42.7.8.jar",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("aws_access_key_id"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("aws_secret_access_key"))
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Get all configurations
    all_spark_configs = spark.sparkContext.getConf().getAll()

    # Print or process the configurations
    for key, value in all_spark_configs:
        print(f"{key}: {value}")

    return spark


def setup_spark_with_minio():
    builder = (
        SparkSession.builder.appName("DeltaLakeS3Local")
        .config(
            "spark.jars",
            "/Users/esak/.ivy2/jars/aws-java-sdk-bundle-1.12.593.jar,/Users/esak/.ivy2/jars/hadoop-aws-3.3.4.jar,/Users/esak/.ivy2/jars/io.delta_delta-spark_2.12-3.3.0.jar,/Users/esak/.ivy2/jars/postgresql-42.7.8.jar",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Get all configurations
    all_spark_configs = spark.sparkContext.getConf().getAll()

    # Print or process the configurations
    for key, value in all_spark_configs:
        print(f"{key}: {value}")

    return spark


def print_dataframe_for_testing(df, table_type):
    num_rows = df.count()
    df.show(n=num_rows, truncate=False, vertical=True)
    print(f"Number of rows in {table_type} table: {num_rows}")


if __name__ == "__main__":
    app_spark = setup_spark_with_aws()
    print(app_spark.version)
    # s3a_path = f"s3a://nexus-datasets/docs/employee.csv"
    products_s3_input_file_path = "s3a://cogesak-etl-target/products.csv"
    products_s3_delta_file_path = (
        "s3a://cogesak-etl-target/cogesak/datasets/lake/products/"
    )
    df = (
        app_spark.read.format("csv")
        .option("header", "true")
        .load(products_s3_input_file_path)
    )
    print(df.printSchema)
    df.show(truncate=False)

    df.write.format("delta").mode("overwrite").save(products_s3_delta_file_path)
