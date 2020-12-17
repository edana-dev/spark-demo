package cn.edana.spark.gio

import org.apache.spark.sql.SparkSession

object ExtractCustomEvent {

  def main(args: Array[String]): Unit = {
    //
    val session = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val df = session.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      //      .load("hdfs://192.168.1.200:9000/spark/custom_event/")
      .load("hdfs://localhost:9000/spark/custom_event/")

    df.show()

    df.printSchema()

    df.select("time", "loginUserId", "eventName", "eventVariable")
      .filter("eventName='ugc_video'")
      .show()

    val cdf = df.select("loginUserId")
      .groupBy("loginUserId")
      .count()

    cdf.orderBy(cdf("count").desc)
      .show(100)


    print("count: ", cdf.filter("count>=10")
      .count())

    val edf = df.select("eventName").groupBy("eventName").count()
    edf.orderBy(edf("count").desc)
      .show(100)

  }

}

