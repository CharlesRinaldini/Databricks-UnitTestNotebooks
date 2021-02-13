import pytest
import json
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from utils import sparkutils, databricksutils, compareutils, configutils

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("OFF")
dbutils = DBUtils(spark)

def test_simple_transform_differences():
    database = "unit_test_db"
    tables = [
        {"Name": "jde_vwBusinessUnitMaster",
         "Path": "/mnt/test/SeedData/Test/seed_BusinessUnitMaster.csv"},
        {"Name": "jde_vwUserDefinedCodeValues",
         "Path": "/mnt/test/SeedData/Test/seed_UserDefinedCodes.csv"}
    ]
    
    seedTables = sparkutils.util(database, tables)
    seedTables.create()

    configs = configutils.util()

    #Run the notebook as a unit test
    notebookInstance = databricksutils.instance(configs.getHost(), configs.getToken(), configs.getClusterId())
    args = {"scopeName" : "sb-databricks-keyvault"
        , "configuration" : "Test"
    }
    notebookInstance.runNotebook("/Shared/TRAN_Simple_Test", args)

    if notebookInstance.getRunResultState() == "SUCCESS":
        #get returns from notebook run
        print(notebookInstance.getRunOutput())
        testNotebookJson = json.loads(notebookInstance.getRunOutput())
        testFilePath = testNotebookJson["FilePath"]
        testStatus = testNotebookJson["ExecutionStatus"]

        if (testStatus == "Pass"):
            # Arrange - limit returned data to what is in assertions
            testFileDF = spark.read.format("parquet").load(testFilePath)
            testFileDF.createOrReplaceTempView("returned_dimSubsidiaryJDE")
            returnedDF = spark.sql("""select 
              subsidiarySK
            , subsidiaryNumber
            , subsidiaryName
            from returned_dimSubsidiaryJDE """)
            # Assert - load expected results
            expectedPath = "/mnt/test/ExpectedData/Test/expected_dimSubsidiary.csv"
            expectedDF = spark.read.format("csv").option("header", "true").option("nullValue", None).option(
                "emptyValue", None).load(expectedPath)
            expectedCount = expectedDF.count()

            # Assert - write out test files
            diffDF = compareutils.diffdataframe(returnedDF, expectedDF, "subsidiarySK")

            assert diffDF.leftNotInRight == 0, f"Failed left not in right. Expected 0 and got {diffDF.leftNotInRight}."
            assert diffDF.rightNotInLeft == 0, f"Failed right not in left. Expected 0 and got {diffDF.rightNotInLeft}."
            assert diffDF.changes == 0, f"Failed changes. Expected 0 and got {diffDF.changes}."
            assert diffDF.sameInBoth == expectedDF.count(), f"Failed same in both. Expected {diffDF.sameInBoth} and got {expectedCount}."