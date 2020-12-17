package cn.edana.spark.quickstart

import org.apache.spark.sql.{SaveMode, SparkSession}


object SparkBatchDemo {
  def main(args: Array[String]): Unit = {
    // 1: input data path
    val hdfsSourcePath = "hdfs://127.0.0.1:9000/user/season/people.json"

    // 2: create SparkSession(SparkContext)
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()
    // 3: read data from dfs
    val df = spark.read.json(hdfsSourcePath)
    // 4.1: Displays the content of the DataFrame to stdout
    df.show()
    df.printSchema()
    df.select("name").show()
    df.select("name").distinct().show()

    // 4.2: Select everybody, but increment the age by 1
    import spark.implicits._
    df.select($"name", $"age" + 1).show()
    val result  = df.groupBy("age").count()

    // 5: write result to hdfs
    val hdfsTargetPath =  "hdfs://127.0.0.1:9000/user/season/people_result.json"
    result.write.mode(SaveMode.Overwrite).json(hdfsTargetPath)

  }
}