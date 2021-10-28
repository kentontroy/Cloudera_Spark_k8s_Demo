from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

def createContext(cores: int, gb: int) -> SparkContext:
  conf = (
    SparkConf()
     .setMaster('local[{}]'.format(cores))
     .setAppName("Cloudera PSE Demo AppLoadHive")
     .set('spark.driver.memory', '{}g'.format(gb))
     .set('hive.metastore.uris', "thrift://kdavis-pse-demo-master0.se-sandb.a465-9q4k.cloudera.site:9083")
  )
  sc = SparkContext(conf=conf)
  sc.setLogLevel("ERROR")
  return sc

def main():
  spark = SparkSession(createContext(1, 2))

  path = "s3a://se-uat2/kdavis-demo/BPD_Part_1_Victim_Based_Crime_Data_post_etl.txt/part-00000-6f4c95fc-bdb7-4675-89ae-1bced5fb1868-c000.csv"
  df = spark.read.option("header", "true").option("delimiter", "\t").csv(path)
  df.createOrReplaceTempView("TRANSFORMED_DATA")

  spark.sql("CREATE TABLE IF NOT EXISTS crime_data as SELECT * FROM TRANSFORMED_DATA")


if __name__ == "__main__":
  main()

