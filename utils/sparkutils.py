from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("OFF")
dbutils = DBUtils(spark)

class util:
    def __init__(self, database, tables):
        self._database = database
        self._tables = tables
        self._options = "('nullValue'='', 'header'='True')"

    def create(self):
        self._createDatabase()
        self._createTables()

    def _createDatabase(self):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self._database}")

    def _createTables(self):
        for table in self._tables:
            tableName = table["Name"]
            tablePath = table["Path"]
            dbCreate = f"DROP TABLE IF EXISTS {self._database}.{tableName}"
            print(dbCreate)
            spark.sql(dbCreate)
            tableCreate = f"CREATE TABLE {self._database}.{tableName} USING CSV OPTIONS {self._options} LOCATION '{tablePath}'"
            print(tableCreate)
            spark.sql(tableCreate)
