import os
from datetime import datetime

from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from src.utils.config_reader import read_config
from src.utils.db_utils import DBUtils
from src.utils.schema import application_schema
from src.utils.spark_utils import setup_spark_with_aws

RUN_DATE = datetime.strptime(os.getenv("RUN_DATE"), "%Y%m%d")
current_timestamp = RUN_DATE.strftime("%Y-%m-%d %H:%M:%S")


def main():
    env = "QA".lower()
    config = read_config(
        f"/Users/esak/Documents/Esakki/code_base/current_project/3LayersArch/configs/qa.yaml"
    )
    print(config)
    app_spark = setup_spark_with_aws()
    db_utils = DBUtils(config, env)
    table_config = read_config(
        f"/Users/esak/Documents/Esakki/code_base/current_project/3LayersArch/configs/tables.yaml"
    )
    application_tables = ["fact_sales", "dim_product", "dim_customer", "dim_store"]
    for rec in application_tables:
        skip = table_config.get(rec).get("skip")
        print(type(table_config.get(rec).get("skip")))
        if skip:
            print(f"we are skipping the table {rec}")
            continue
        print(f"we are processing the table {rec}")

        table_name = rec
        table_configurations = table_config.get(rec)
        full_table_population = table_config.get(table_name).get("full_load_table")
        num_partition = int(table_config.get(table_name).get("num_partition"))
        table_path = f"{table_config.get(rec).get('staging_table').get("path").get(env)}{table_name}"
        update_timestamp_col = f"{table_config.get(rec).get('update_timestamp_col')}"
        max_timestamp_value = get_max_timestamp(
            app_spark, table_path, update_timestamp_col
        )
        print(
            f"Maximum Timestamp Value {max_timestamp_value} for the table {table_name}"
        )
        staging_population_query = get_staging_population(
            table_name, full_table_population, update_timestamp_col, max_timestamp_value
        )
        # print(staging_population_query)
        partition_configs = get_partition_configs(
            app_spark,
            db_utils,
            table_name,
            "row_number",
            staging_population_query,
            num_partition,
        )
        delete_population = delete_population_query(
            app_spark,
            table_name,
            update_timestamp_col,
            max_timestamp_value,
            table_configurations,
            db_utils,
        )
        staging_data_df, expected_final_staging_count = process_staging_table(
            app_spark,
            partition_configs,
            table_configurations,
            staging_population_query,
            delete_population,
            db_utils,
        )

        staging_data_df.printSchema()
        app_schema = application_schema.get(f"{table_name}_schema")
        staging_data_with_schema_df = standardize_schema(staging_data_df, app_schema)
        # write the data to the delta lake
        staging_data_with_schema_df.write.format("delta").mode("overwrite").save(
            table_path
        )

        staging_delta_table = DeltaTable.forPath(app_spark, table_path)
        staging_table_count = staging_delta_table.toDF().count()
        print(f"Staging Table Count {staging_table_count}")

        if expected_final_staging_count != staging_table_count:
            rollback_delta_table(staging_delta_table)
            staging_data_df.unpersist()
            print("Staging Table Count Mismatch")
            raise Exception("Staging Table Count Mismatch")
        else:
            print(
                f"Staging Table Count { staging_table_count} is matching with the expected {expected_final_staging_count} Correct"
            )

        staging_data_df.unpersist()
        print(f"Table {table_name} loaded into the Delta Staging Path Successfully")

    print(f"All the Application Tables loaded into the Delta Staging Path Successfully")


def rollback_delta_table(table):
    previous_version = table.history().select("version").collect()[1][0]
    table.restoreToVersion(previous_version)


def get_max_timestamp(spark: SparkSession, table_path: str, update_timestamp_col: str):
    df = spark.read.format("delta").load(table_path)
    print(df.printSchema())
    df = df.select(
        coalesce(max(col(update_timestamp_col)), lit("1900-01-01 00:00:00")).alias(
            "max_time_stamp"
        )
    )
    df.show()
    return df.first()[0]


def get_staging_population(
    table_name: str,
    full_table_load_type: bool,
    update_timestamp_col: str,
    max_time_stamp: str,
) -> str:
    """Get the Staging Population from the postgres database"""
    """
    l. Is load Type Full_load wha will be the query
    2. If the type is delta load what wil be the query
    """
    if full_table_load_type and update_timestamp_col:
        print(
            f"we are preparing for the FULL LOAD TYPE as {full_table_load_type} for table {table_name}"
        )
        return f"select * from products.{table_name} where {update_timestamp_col} <= '{current_timestamp}'::timestamp"
    elif full_table_load_type:
        print(
            f"we are preparing for the FULL LOAD TYPE as {full_table_load_type} for table {table_name}"
        )
        return f"select * from products.{table_name}"
    else:
        print(
            f"we are preparing for the FULL LOAD TYPE as {full_table_load_type} for table {table_name}"
        )
        return (
            f"select * from products.{table_name} "
            f"where {update_timestamp_col} BETWEEN '{max_time_stamp}'::timestamp + interval '1 days' and '{current_timestamp}'::timestamp "
        )


def delete_population_query(
    spark: SparkSession,
    table_name: str,
    update_timestamp_col: str,
    max_time_stamp: str,
    table_config: dict,
    db_utils: DBUtils,
):
    # Calculate the delete population
    primary_keys = ",".join(table_config.get("primary_keys"))
    audit_table = f"{table_name}_audit"
    audit_seq_id = table_config.get("audit_seq_id", "NA")
    delete_population_query = f"""( select {primary_keys} from (
                select {primary_keys} , operation_type, rank() over ( partition by {primary_keys} order by {audit_seq_id} desc
            )  from products.{audit_table} where {update_timestamp_col} >= '{max_time_stamp}'::timestamp - interval '1 days' ) audit_table
            where rank = 1 and operation_type = 'D' ) as delete_population
            """

    df = db_utils.read_from_database(spark, delete_population_query)

    df.show()

    return delete_population_query


def get_partition_configs(
    spark: SparkSession,
    db_utils: DBUtils,
    table_name: str,
    partition_key: str,
    staging_population: str,
    num_partition: int,
):
    partition = partition_key
    is_row_number_partitions = False
    number_of_partitions = num_partition
    if partition_key != "row_number":
        upper_bound_query = f"SELECT MIN({partition_key}) , MAX({partition_key} FROM ({staging_population})"
        upper_bound_results = db_utils.read_from_database(spark, upper_bound_query)
        lower_bound = upper_bound_results.first()["min"]
        upper_bound = upper_bound_results.first()["max"]
    else:
        upper_lower_bound_query = f"(select COUNT(*) from ( {staging_population} ) as stg_population_count ) as final_population"
        print(upper_lower_bound_query)
        incremental_data_count = db_utils.read_from_database(
            spark, upper_lower_bound_query
        )
        incremental_data_count.show()
        lower_bound = 1
        upper_bound = incremental_data_count.first()["count"]
        partition = ""
        is_row_number_partitions = True
        number_of_partitions = 1

    print(
        (
            upper_bound,
            lower_bound,
            partition,
            number_of_partitions,
            is_row_number_partitions,
        )
    )

    return {
        "upper_bound": upper_bound,
        "lower_bound": lower_bound,
        "partition": partition,
        "num_partition": num_partition,
        "is_row_number_partitions": is_row_number_partitions,
    }


def union_audit_delete_dataframe(
    staging_df: DataFrame, delete_primary_keys: DataFrame
) -> DataFrame:
    audit_table_deletes = delete_primary_keys.withColumn("is_deleted", lit(True))
    for columnx in staging_df.columns:
        if columnx not in audit_table_deletes.columns:
            audit_table_deletes = audit_table_deletes.withColumn(columnx, lit(None))
    final_df = staging_df.unionByName(audit_table_deletes)
    final_df.show()
    final_df.printSchema()
    return final_df


def process_staging_table(
    spark: SparkSession,
    partition_configs: dict,
    table_config: dict,
    staging_population_query: str,
    delete_population_query: str,
    db_utils: DBUtils,
):
    primary_keys = ",".join(table_config.get("primary_keys"))
    if partition_configs.get("is_row_number_partitions"):
        staging_population_query = f"(SELECT *, ROW_NUMBER() OVER (ORDER BY {primary_keys}) as row_number FROM ( {staging_population_query} ) as part_stg)as final_stg"

    staging_df = db_utils.read_from_database(
        spark,
        staging_population_query,
        partition_key=partition_configs.get("partition"),
        upper_bound=partition_configs.get("upper_bound"),
        lower_bound=partition_configs.get("lower_bound"),
        num_partition=partition_configs.get("num_partition"),
    )
    expected_final_count = staging_df.count()
    staging_df = staging_df.withColumn("is_deleted", lit(False))

    # Executing the Delete Query to get the delete primary keys Population
    delete_primary_keys = db_utils.read_from_database(spark, delete_population_query)

    if len(delete_primary_keys.head(1)) > 0:
        print(
            f"we are having delete Populations , so we are adding the delete records to the staging table"
        )
        audit_table_deletes_count = delete_primary_keys.count()
        expected_final_count = expected_final_count + audit_table_deletes_count
        staging_df = union_audit_delete_dataframe(staging_df, delete_primary_keys)

    staging_df.show()
    staging_df.cache()

    return staging_df, expected_final_count


def standardize_schema(df: DataFrame, src_schema: StructType):
    return df.select(*[(col(x.name).cast(x.dataType)) for x in src_schema.fields])


if __name__ == "__main__":
    main()
