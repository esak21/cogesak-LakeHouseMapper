from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from src.utils.config_reader import read_config
from src.utils.db_utils import DBUtils
from src.utils.spark_utils import setup_spark_with_aws

DELETE_FLAG = "is_deleted"


def build_merge_parameters(staging_df: DataFrame, primary_keys: list) -> dict:
    # Get all the table Columns expected the is_delete column

    table_columns = [
        column
        for column in staging_df.columns
        if column not in ["is_deleted", "row_number"]
    ]
    non_primary_table_columns = [
        column for column in table_columns if column not in primary_keys
    ]

    merge_condition = " AND ".join(
        [f"source.{column} = target.{column}" for column in primary_keys]
    )

    # if any of the non-primary fields are distinct we update these columns
    filed_updated_condition = " OR ".join(
        [
            f"source.{column} IS DISTINCT FROM target.{column}"
            for column in non_primary_table_columns
        ]
    )

    insert_field_dict = {f"{column}": f"source.{column}" for column in table_columns}

    update_fields_dict = {
        f"{column}": f"source.{column}" for column in non_primary_table_columns
    }

    merge_bc_condition = {
        "merge_condition": merge_condition,
        "update_fields_dict": update_fields_dict,
        "insert_field_dict": insert_field_dict,
        "filed_updated_condition": filed_updated_condition,
    }

    return merge_bc_condition


def process_full_table_load(
    staging_df: DataFrame, bc_table: DeltaTable, merge_parameters: dict
) -> int:
    if merge_parameters.get("merge_condition") is None:
        raise ValueError("Merge Condition is not provided")

    expected_records_count = staging_df.count()
    (
        bc_table.alias("target")
        .merge(
            staging_df.alias("source"),
            condition=merge_parameters.get("merge_condition"),
        )
        .whenMatchedUpdate(
            condition=merge_parameters.get("filed_updated_condition"),
            set=merge_parameters.get("update_fields_dict"),
        )
        .whenNotMatchedInsert(values=merge_parameters.get("insert_field_dict"))
        .whenNotMatchedBySourceDelete()
        .execute()
    )

    return expected_records_count


def process_incremental_table_load(
    staging_df: DataFrame,
    bc_table: DeltaTable,
    primary_keys: list[str],
    merge_parameters: dict,
) -> int:
    target = bc_table.toDF().alias("target")
    source = staging_df.alias("source")

    # get insert Count
    insert_record_count = (
        source.join(target, on=primary_keys, how="leftanti")
        .where(f"not source.{DELETE_FLAG}")
        .count()
    )
    # Get Delete Count
    deleted_record_count = (
        source.join(
            target,
            on=primary_keys,
            how="inner",
        )
        .where(f"source.{DELETE_FLAG}")
        .count()
    )

    expected_record_count = target.count() + insert_record_count - deleted_record_count

    bc_table.alias("target").merge(
        staging_df.alias("source"), condition=merge_parameters.get("merge_condition")
    ).whenNotMatchedInsert(
        condition=f"not source.{DELETE_FLAG}",
        values=merge_parameters.get("insert_field_dict"),
    ).whenMatchedUpdate(
        condition=merge_parameters.get("filed_updated_condition"),
        set=merge_parameters.get("update_fields_dict"),
    ).whenMatchedDelete(condition=f"source.{DELETE_FLAG}").execute()

    return expected_record_count


def validate_bc_record_count(
    bc_table_count: int, expected_record_count: int, table_name: str
) -> bool:
    if bc_table_count != expected_record_count:
        raise ValueError("BC table count does not match")
        return False
    else:
        print(
            f"Table {table_name} merge successsfully with expected record count {expected_record_count}"
        )
        return True


def rollback_merge_into_bc(
    app_spark: SparkSession, bc_table: DeltaTable, table_name: str, db_utils: DBUtils
):
    version_history = bc_table.history().select("version").collect()
    prev_version = version_history[1][0]
    bc_table.restoreToVersion(version=prev_version)
    update_query = (
        f"UPDATE products.version_tracking_ivt "
        f"SET skip_versions = {prev_version}"
        f"WHERE Ttable_name = {table_name}"
    )

    write_result = db_utils.write_to_database(
        spark_session=app_spark, query=update_query, table_name=table_name
    )
    if not write_result:
        raise Exception("Failed to write to database")
    else:
        print(
            f"Table {table_name} rolled back successfully and metadata logged into  tracking table"
        )


def main():
    """Let's Merge the Data from the Staging Table to the Business Current Delta Table"""

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
        business_current_path = f"{table_config.get(table_name).get("business_current_table").get("path").get(env)}{table_name}"
        staging_path = f"{table_config.get(table_name).get("staging_table").get("path").get(env)}{table_name}"
        full_load_table = table_config.get(table_name).get("full_load_table")
        primary_keys = table_config.get(table_name).get("primary_keys")
        print(primary_keys)
        print(type(primary_keys))
        update_timestamp_col = table_config.get(table_name).get("update_timestamp_col")

        bc_delta_table = DeltaTable.forPath(app_spark, business_current_path)
        staging_df = app_spark.read.format("delta").load(staging_path)
        print(staging_df.printSchema())
        staging_df.show()

        merge_parameters = build_merge_parameters(staging_df, primary_keys)

        print(merge_parameters)

        if full_load_table:
            print(f"Table {table_name} With Full load option as {full_load_table}")
            expected_record_count = process_full_table_load(
                staging_df, bc_delta_table, merge_parameters
            )
        else:
            print(
                f"Table {table_name} With Incremental load option as {full_load_table}"
            )
            expected_record_count = process_incremental_table_load(
                staging_df, bc_delta_table, primary_keys, merge_parameters
            )

        bc_table_count = bc_delta_table.toDF().count()

        is_success = validate_bc_record_count(
            bc_table_count, expected_record_count, table_name
        )

        if not is_success:
            rollback_merge_into_bc(app_spark, bc_delta_table, table_name, db_utils)
            raise Exception(
                "Expected counts don't match after merge rolling bc table back"
            )

        print(f"table {table_name} Executed successfully")


if __name__ == "__main__":
    main()
