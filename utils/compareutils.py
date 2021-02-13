from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from pyspark.sql import functions as F

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("OFF")
dbutils = DBUtils(spark)

class diffdataframe:

    def __init__(self, leftDF, rightDF, joinKeys):
        self.leftDF = leftDF
        self.rightDF = rightDF
        #comma separated list
        self.joinKeys = joinKeys.split(",")
        self.leftNotInRight = None
        self.rightNotInLeft = None
        self.sameInBoth = None
        self.changes = None
        self._doDiff()

    def _doDiff(self):
        self.leftNotInRight = self._setLeftNotInRight()
        self.rightNotInLeft = self._setRightNotInLeft()
        self.changes = self._setChanges()
        self.sameInBoth = self._setSameInBoth()
        
    def _setLeftNotInRight(self):
        cols = self.joinKeys
        leftNotInRightDF = self.leftDF.join(self.rightDF, cols, "left_anti")
        return leftNotInRightDF.count()
    
    def _setRightNotInLeft(self):
        cols = self.joinKeys
        rightNotInLeftDF = self.rightDF.join(self.leftDF, cols, "left_anti")
        return rightNotInLeftDF.count()
    
    def _setSameInBoth(self):
        sameInBothDF = self.leftDF.intersect(self.rightDF)
        return sameInBothDF.count()
    
    def _setChanges(self):
        cols = self.joinKeys
        columns = self.leftDF.columns
        # get conditions for all columns except id
        conditions_ = [F.when(self.leftDF[c]!=self.rightDF[c], F.lit(c)).otherwise("") for c in self.leftDF.columns if c not in cols]
        
        select_expr =[
                        *cols,
                        *[self.rightDF[c] for c in self.rightDF.columns if c not in cols],
                        F.array_remove(F.array(*conditions_), "").alias("column_names")
        ]
        
        changesDF = (self.leftDF.join(self.rightDF, cols, "left").select(*select_expr)).where("size(column_names) > 0")
        return changesDF.count()





