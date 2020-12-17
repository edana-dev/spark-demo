package cn.edana.spark.quickstart

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object FileStreamingDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("file-streaming-demo")
      .setMaster("local[1]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val ssc = new StreamingContext(sc, Seconds(10))


    // 通过指定hdfs的目录来创建流
    val hdfsPath = "hdfs://127.0.0.1:9000/spark/data/"
    // 返回的Stream类型为`InputDStream[(LongWritable, Text)]`
    val stream = ssc.fileStream[LongWritable, Text, TextInputFormat](hdfsPath)

    // 逻辑处理
    stream
      .map(item => {
        item._2.toString
      })
      .map(_.split(" "))
      .map(arr => (arr(0), 1))
      .reduceByKey(_+_)
      .print(100)

    ssc.start()
    ssc.awaitTermination()
  }

}
