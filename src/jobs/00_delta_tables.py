from src.utils.config_reader import read_config
from src.utils.schema import *
from src.utils.spark_utils import setup_spark_with_aws


def main():
    app_spark = setup_spark_with_aws()
    env = "qa"
    section = "db"
    config = read_config(
        f"/Users/esak/Documents/Esakki/code_base/current_project/3LayersArch/configs/tables.yaml"
    )
    print(config)
    application_tables = ["dim_customer", "dim_product", "dim_store", "fact_sales"]
    for table in application_tables:
        app_schema_name = config.get(table).get("schema_id").get(env)
        app_schema = application_schema.get(app_schema_name)
        print(app_schema)
        staging_table_path = (
            f"{config.get(table).get("staging_table").get("path").get(env)}{table}"
        )
        df = app_spark.createDataFrame(data=[], schema=app_schema)
        df.printSchema()
        df.show()
        # Write it for only one time to create the table
        df.write.format("delta").mode("overwrite").save(staging_table_path)


if __name__ == "__main__":
    main()
