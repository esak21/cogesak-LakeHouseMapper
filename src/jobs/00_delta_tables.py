from src.utils.config_reader import read_config
from src.utils.schema import *
from src.utils.spark_utils import setup_spark_with_aws


def main():
    # change the Below Value for Running staging and bc tables
    # business_current or staging
    table_env = "staging"
    create_table = False
    env = "qa"
    section = "db"

    app_spark = setup_spark_with_aws()
    config = read_config(
        f"/Users/esak/Documents/Esakki/code_base/current_project/3LayersArch/configs/tables.yaml"
    )
    print(config)
    application_tables = ["dim_customer", "dim_product", "dim_store", "fact_sales"]

    table_run_details = {
        "staging": {"app_schema": application_schema, "table_desc": "staging_table"},
        "business_current": {
            "app_schema": bc_application_schema,
            "table_desc": "business_current_table",
        },
    }
    for table in application_tables:
        table_env_desc = table_run_details.get(table_env).get("table_desc")
        app_schema_name = config.get(table).get(table_env_desc).get("schema")
        print(app_schema_name)
        app_schema = (
            table_run_details.get(table_env).get("app_schema").get(app_schema_name)
        )

        table_path = (
            f"{config.get(table).get(table_env_desc).get("path").get(env)}{table}"
        )
        print(app_schema)
        if create_table:
            df = app_spark.createDataFrame(data=[], schema=app_schema)
            df.printSchema()
            df.show()
            # # Write it for only one time to create the table
            df.write.format("delta").mode("overwrite").save(table_path)
            # df.write.format("delta").mode("overwrite").save(bc_table_path)

        # Reading the delta lake from the path and display the schema
        stg_df = app_spark.read.format("delta").load(table_path)
        print(f" :::::::::::Printing the Staging table :::::::::::")
        stg_df.show()
        stg_df.printSchema()

        print(f" :::::::::::Printing the Business Current table :::::::::::")
        bc_table_path = f"{config.get(table).get("business_current_table").get("path").get(env)}{table}"
        bc_df = app_spark.read.format("delta").load(bc_table_path)
        bc_df.show()
        bc_df.printSchema()


if __name__ == "__main__":
    main()
