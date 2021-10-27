package com.cloudera.pse.demo

import collection.JavaConverters._
import java.util.UUID.randomUUID
import org.apache.kudu.client._
import org.apache.kudu.spark.kudu.{KuduContext, KuduWriteOptions}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.slf4j.LoggerFactory

object AppETLCrimeData {
  val logger = LoggerFactory.getLogger(AppETLCrimeData.getClass)
  val kuduMasters: String = System.getProperty("KUDU_MASTERS", "cdp:7051")
  val tableName: String = System.getProperty("TABLE_NAME", "default.bpd_crime_data")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("AppETLCrimeData")
      .config("spark.master", "local")
      .config("spark.driver.memory", "4g") 
      .config("spark.executor.memory", "4g")
      .getOrCreate()

    spark.conf.set("spark.sql.files.maxRecordsPerFile", 300000)

    import spark.implicits._

    val kuduContext = new KuduContext(kuduMasters, spark.sqlContext.sparkContext)

    try {
      if (!kuduContext.tableExists(tableName)) {
        throw new RuntimeException(tableName + ": does not exist")
      }
      logger.info(s"Found table: " + tableName)

     val df = spark.read.option("header", "true").option("delimiter", "\t")
                .csv("file:///home/centos/data/BPD_Part_1_Victim_Based_Crime_Data_tab.txt")

     val uuid = udf(() => java.util.UUID.randomUUID().toString)
     val keyedDf = df.withColumn("id", uuid())

     val latitude = (v: String) => {
       if (v == null || v.trim().length()==0)
         -1
       else 
         v.replaceAll("[()]+", "").split(",")(0).trim().toDouble
     }

     val longitude = (v: String) => {
       if (v == null || v.trim().length()==0)
         -1
       else
         v.replaceAll("[()]+", "").split(",")(1).trim().toDouble
     } 

     val year = (v: String) => {
       if (v == null || v.trim().length()==0)
         -1
       else
         v.split("/")(2).trim().toInt
     }

     spark.udf.register("latitude", latitude)
     spark.udf.register("longitude", longitude)
     spark.udf.register("year", year) 

     keyedDf.createOrReplaceTempView("UNPARSED_LAT_LON")

     val sql = """
       SELECT id, year(crimedate) AS crimeyear, crimecode, crimedate, crimetime, 
              latitude(latlon) AS lat, longitude(latlon) AS lon,
              location AS address, description, insideflag, weapon, post, district, neighborhood, total
       FROM UNPARSED_LAT_LON
     """
     val finalDf = spark.sql(sql)
     finalDf.printSchema()
     finalDf.show(10)

     val path = "file:///home/centos/data/BPD_Part_1_Victim_Based_Crime_Data_transformed.parquet"
     finalDf.repartition(1).write.partitionBy("crimeyear").parquet(path)

     kuduContext.insertRows(finalDf, tableName, new KuduWriteOptions(ignoreDuplicateRowErrors = true))
     
   }
   catch {
     case unknown : Throwable => logger.error(s"Exception occurred: " + unknown)
   } 
   finally { 
     logger.info(s"Session closing")
     spark.close()
   }
  }
}

