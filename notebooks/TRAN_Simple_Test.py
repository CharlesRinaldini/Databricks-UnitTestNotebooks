# Databricks notebook source
# DBTITLE 1,Library Imports
#Spark SQL imports
from pyspark.sql.types import *
from pyspark.sql.functions import *

#datetime functions for folder and file naming 
from datetime import datetime

#imports for running stored procedures
import pyodbc

#import for creating output of notebook
import json

#import for interacting with Delta Lake
import delta

# COMMAND ----------

# DBTITLE 1,Setup Widgets
#widget to determine run mode
dbutils.widgets.dropdown("configuration", "Debug", ["Debug", "Release", "Test"], "Configuration")

# COMMAND ----------

# DBTITLE 1,Variable Loading
#get run information #todo: create actual ephemeral notebook url
notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
try:
  #The tag jobId does not exists when the notebook is not triggered by dbutils.notebook.run(...) 
  pipelineOrNotebookName = str(notebook_info["tags"]["jobId"])
except:
  pipelineOrNotebookName = "-1"
  
#get secrets
scopeName = dbutils.widgets.get("scopeName")
config = dbutils.widgets.get("configuration")

#create mounts
testMnt = "/mnt/test"
tempMnt = "/mnt/temp"
goldMnt = "/mnt/gold"

#set date and time values for paths and file names
now = datetime.now()
modifiedDatetime = now.strftime("%Y-%m-%d %H:%M:%S")

#set prefixes to be used in sqls later on
if config == "Test":
  jde_dwDbName = "unit_test_db.jde_"
else:
  jde_dwDbName = "jde_dw."

oldWatermark = "0"
#file date used when creating a test output
now = datetime.now()
folderHNS = now.strftime("yyyy=%Y/MM=%m/dd=%d/")
fileDatetime = now.strftime("_%Y%m%d%H%M%S.parquet")
isError = False

targetEntityName = "DW.dimSimple"
tempViewName = "temp_dimSimple"
goldPath = goldMnt + targetEntityName

# COMMAND ----------

# DBTITLE 1,Functions
#writeDataLakeFile handles removing the original file folder, creating a new file, and passing back the new file path that was created.
def writeDatalakeFileToTest(dataFrame, fileSystem, storagePath, entityName, databaseName):
  newFile = ""
  try:
    dataFrame.cache()
    tempPath = tempMnt + "/" + entityName + "_" + databaseName + "_temp/" + entityName + ".parquet"
    dbutils.fs.rm(tempPath, True)
    testPath = testMnt + "/" + storagePath
    #this operation is done across all nodes and caches the results to memory
    dataFrameCount = dataFrame.count()
    #only create a file if there are actual changes
    if(dataFrameCount > 0):
      print("Saving to: temp")
      #this operation is done on the head node to create only a single file
      dataFrame.coalesce(1).write.mode("overwrite").parquet(tempPath)
      #get a list of all files created 
      rawFiles = dbutils.fs.ls(tempPath)
      #loop the files and find the single parquet file 
      for f in rawFiles:
        if f.name.find(".parquet") > 0:
          tempFile = f.name
      #declare the path where the temp file needs to be moved to 
      newFile = testPath + entityName + fileDatetime
      #move the temp file to the actual path
      dbutils.fs.mv(tempPath + "/" + tempFile, newFile, recurse= True)
      print("Created file: " + newFile)
      #return the new path of the dataframe
      returnValue = '{"FileName": "'+newFile+'"}'
    else:
      print("Empty dataframe")
      returnValue = '{"FileName": "NoFile"}'
  except Exception as e:
      print(e)
      return "Error"
  
  return returnValue

# COMMAND ----------

# DBTITLE 1,Set SQLs used in Creating Delta Tables
viewSql = """
SELECT DISTINCT
    conv(right(sha1(encode(bum.Subsidiary, 'UTF-16LE')),16),16,-10) as subsidiarySK,  
    bum.Subsidiary as subsidiaryNumber,
    udcv.Description AS subsidiaryName,
    current_timestamp() AS audCreateDateTime,
    current_timestamp() AS audUpdateDateTime
FROM """ + jde_dwDbName + """vwBusinessUnitMaster AS bum 
LEFT JOIN """ + jde_dwDbName + """vwUserDefinedCodeValues AS udcv
    ON udcv.userDefinedCodes = '18' 
    AND udcv.productCode = '00' 
    AND udcv.userDefinedCode = bum.Subsidiary
  """

# COMMAND ----------

# DBTITLE 1,Create or Update Gold Zone
try:        
  #create a temp view for processing into destination delta table with the correct data structure
  newRecordsDF = spark.sql(viewSql)
  newRecordsDF.createOrReplaceTempView(tempViewName)

  if config == "Test":
    #Write whole view to test zone
    print("Running in test mode")
    dbName = (targetEntityName.split("."))[0]
    entityName = (targetEntityName.split("."))[1]
    testPath = "/OutputData/"+dbName+"/"+entityName+"/" + folderHNS
    testFileJson = json.loads(writeDatalakeFileToTest(newRecordsDF, "test", testPath, targetEntityName, "test"))
    targetPath = testFileJson["FileName"]
  else:    
    newRecordsDF.cache()
    if newRecordsDF.count() > 0:   
      #get the first part of the target entity name
      databaseName = (targetEntityName.split("."))[0]
      #if you create a delta lake folder with a prefix it's considered the database name 
      dbSql = "CREATE DATABASE IF NOT EXISTS " + databaseName
      #create the database if it doesn't exist - this will keep things organized in the future 
      spark.sql(dbSql)      
      print("Saving records to data lake...")
      #save the new records as a delta table - note: there is no way to enforce schema here
      newRecordsDF.write.format("delta").mode("overwrite").save(goldPath)
      print("Registering delta table...")
      #create the delta table from the delta folder if it hasn't been created already
      #note: don't create a table with a schema because merging into later will throw an error for nullables
      spark.sql("CREATE TABLE IF NOT EXISTS "+targetEntityName+" USING DELTA LOCATION '"+goldPath+"'")
      targetPath = goldPath
    else:
      print("No new records to merge.")
except BaseException  as e:
  ErrorMessage = str(e)
  #if there was and error after opening the watermark then log the error
  print(e)
  outJson = '{"Configuration":"' + config + '","ExecutionStatus":"'+ErrorMessage.replace('"', "")+'","FilePath":"None"}'
  isError = True

# COMMAND ----------

if isError:
  dbutils.notebook.exit(outJson)

# COMMAND ----------

#TODO: Exit notebook reporting back what tables were updated
outJson = '{"Configuration":"' + config + '","ExecutionStatus":"Pass","FilePath":"' + targetPath + '"}'
dbutils.notebook.exit(outJson)
