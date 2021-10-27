import re
import uuid
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import udf
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType, IntegerType, DoubleType

def createContext(cores: int, gb: int) -> SparkContext:
  conf = (
    SparkConf()
     .setMaster('local[{}]'.format(cores))
     .setAppName("Cloudera PSE Demo AppETLCrimeData")
     .set('spark.driver.memory', '{}g'.format(gb))
  )
  sc = SparkContext(conf=conf)
  sc.setLogLevel("ERROR")
  return sc

def uuid(v):
  return uuid.uuid4()

def latitude(v: str):
  if (v is None or len(v.strip())==0):
    return -1
  return float(re.sub("[()]+", "", v).split(",")[0].strip())

def longitude(v: str):
  if (v is None or len(v.strip())==0):
    return -1
  return float(re.sub("[()]+", "", v).split(",")[1].strip())

def year(v: str):
  if (v is None or len(v.strip())==0):
    return -1
  return int(v.split("/")[2].strip())

def main():
  spark = SparkSession(createContext(1, 2))
  df = spark.read.option("header", "true").option("delimiter", "\t").csv("s3a://statisticalfx/Data/Public Safety/BPD_Part_1_Victim_Based_Crime_Data_tab.txt")
  df.createOrReplaceTempView("RAW_DATA")

  uuidUDF = udf(lambda v: uuid(v))
  spark.udf.register("uuidUDF", uuid, StringType())

  latUDF = udf(lambda v: latitude(v))
  spark.udf.register("latUDF", latitude, DoubleType())

  lonUDF = udf(lambda v: longitude(v))
  spark.udf.register("lonUDF", longitude, DoubleType())
  
    yearUDF = udf(lambda v: year(v))
  spark.udf.register("yearUDF", year, IntegerType())

  sql = """
       SELECT uuid() AS id, yearUDF(crimedate) AS crimeyear, crimecode, crimedate, crimetime,
              latUDF(latlon) AS lat, lonUDF(latlon) AS lon,
              location AS address, description, insideflag, weapon, post, district, neighborhood, total
       FROM RAW_DATA
  """

  finalDf = spark.sql(sql)
  finalDf.printSchema()
  finalDf.show(10)

if __name__ == "__main__":
  main()
  
  
