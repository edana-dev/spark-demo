package cn.edana.spark.quickstart

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.parsing.json.JSON

object SparkStreamingDemo {

  def main(args: Array[String]): Unit = {
    try {
      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "localhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "spring-stream-cg",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (true: java.lang.Boolean)
      )
      val topics = Array("spark-topic-1", "spark-topic-2")
      val conf = new SparkConf()
        .setAppName(this.getClass.getSimpleName)
        .setMaster("local")

      val streamingContext = new StreamingContext(conf, Seconds(10))
      val checkPointDirectory = "hdfs://127.0.0.1:9000/spark/checkpoint"
      streamingContext.checkpoint(checkPointDirectory)
      val stream = KafkaUtils.createDirectStream[String, String](
        streamingContext,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )

      val etlResultDirectory = "hdfs://127.0.0.1:9000/spark/etl/"
      val etlRes = stream.map(record => record.value())
//        .filter(m => None != JSON.parseFull(m))
      etlRes.count().print()
      etlRes.saveAsTextFiles(etlResultDirectory)

      streamingContext.start()
      streamingContext.awaitTermination()
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        System.err.println("exception===>: ...")
      }
    }
  }

}
