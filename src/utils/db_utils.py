import copy
import re

from pyspark.sql import SparkSession

from src.utils.config_reader import read_config
from src.utils.spark_utils import setup_spark_with_aws


class DBUtils:
    """Common Methods used for connecting with the DB"""

    def __init__(self, config: dict, env: str):
        self.config = config
        self.env = env
        self.connection_properties = self.get_connection_properties()
        self.jdbc_url = self.get_jdbc_url()
        self.id_pattern = re.compile(r"[A-Za-z0-9-_]+")

    def get_connection_properties(self):
        db_env_config = self.config["db"]
        # In production we need to Fetch the user name and password from any Secret managers
        # AWS we use secrets Manager to store the Credentials
        # TODO Ask Esakki Add Code to retrive the password from secret manager
        return {
            "user": db_env_config["database_username"],
            "password": db_env_config["database_password"],
            "driver": "org.postgresql.Driver",
        }

    def get_jdbc_url(self):
        DATABASE_ENDPOINT = self.config["db"].get("database_endpoint")
        DATABASE_PORT = self.config["db"].get("database_port")
        POSTGRES_DB = self.config["db"].get("database_name")
        return f"jdbc:postgresql://{DATABASE_ENDPOINT}:{DATABASE_PORT}/{POSTGRES_DB}"

    def read_from_database(
        self,
        spark_session: SparkSession,
        population_query: str,
        partition_key: str = None,
        upper_bound: int = None,
        lower_bound: int = None,
        num_partition: int = None,
    ):
        """
        Reads data from a Postgresql database using a SparkSession and
        returns a DataFrame.

        Parameters
        ----------
        spark_session : SparkSession
            The SparkSession to use for reading the data.
        population_query : str
            The SQL query to use for reading the data.
        partition_key : str, optional
            The column to use for parallelizing the read operation.
            Defaults to None.
        upper_bound : int, optional
            The upper bound of the partition.
            Defaults to None.
        lower_bound : int, optional
            The lower bound of the partition.
            Defaults to None.
        num_partition : int, optional
            The number of partitions to use for parallelizing the read operation.
            Defaults to None.

        Returns
        -------
        DataFrame
            The DataFrame containing the read data.
        """
        read_connection_properties = copy.deepcopy(self.connection_properties)
        read_connection_properties["url"] = self.jdbc_url
        read_connection_properties["dbtable"] = population_query

        # we can increase the performance by adding parallelism
        if partition_key:
            read_connection_properties["partitionColumn"] = partition_key
            read_connection_properties["numPartitions"] = num_partition
            read_connection_properties["lowerBound"] = lower_bound
            read_connection_properties["upperBound"] = upper_bound

        print(read_connection_properties)
        df = (
            spark_session.read.format("jdbc")
            .options(**read_connection_properties)
            .load()
        )

        return df

    def sanitize_parameters(self, parameter_list: list):
        for param in parameter_list:
            if not re.fullmatch(self.id_pattern, param):
                raise ValueError(f"Parameter {param} is not a valid parameter")
        return True


if __name__ == "__main__":
    env = "dev"
    section = "db"
    config = read_config(
        f"/Users/esak/Documents/Esakki/code_base/current_project/3LayersArch/configs/{env}.yaml"
    )
    print(config[section])
    db_utils = DBUtils(config, env)
    app_spark = setup_spark_with_aws()
    print(db_utils.get_connection_properties())
    df = db_utils.read_from_database(
        app_spark,
        "( select * from analytics_db.public.dim_store) q",
        partition_key="store_key",
        num_partition=3,
        upper_bound=3,
        lower_bound=1,
    )
    print(df)
    df.show()
