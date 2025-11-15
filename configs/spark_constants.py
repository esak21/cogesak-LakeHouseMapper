from pyspark.sql.types import *

DATA_TYPE_TO_STRUCT_TYPE_DICT = {
    "STRING": StringType(),
    "INT": IntegerType(),
    "LONG": LongType(),
    "FLOAT": FloatType(),
    "DOUBLE": DoubleType(),
    "BOOLEAN": BooleanType(),
    "DATE": DateType(),
    "TIMESTAMP": TimestampType(),
    "DECIMAL": DecimalType(25, 3),
}
