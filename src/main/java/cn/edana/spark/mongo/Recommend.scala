package cn.edana.spark.mongo

import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.SparkSession

object Recommend {

  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("MongoDemo")
      .master("local")
      //      .config("spark.mongodb.auth.uri", "mongodb://root:123%25abc@119.23.34.178:27017/admin")
      //      .config("spark.mongodb.input.uri", "mongodb://119.23.34.178:27017/recommend.gio_custom_event_202012")
      .getOrCreate()


    val model: ALSModel = ALSModel.load("/tmp/mongo-demo/model")

    val usersDf = spark.read.parquet("/tmp/mongo-demo/users.parquet")
    val itemsDf = spark.read.parquet("/tmp/mongo-demo/items.parquet")
    usersDf.createTempView("users")
    itemsDf.createTempView("items")


    val itemRecs = model.recommendForAllItems(10)
    itemRecs.show()

    val userRecs = model.recommendForAllUsers(10)
    userRecs.show()

    userRecs.createTempView("user_recs")

    val userRecsDf = spark.sql("select user_recs.userId, userCode, recommendations from user_recs join users on user_recs.userId = users.userId")
    userRecsDf.show()


  }
}
