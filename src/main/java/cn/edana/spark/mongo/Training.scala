package cn.edana.spark.mongo

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

object Training {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("MongoDemo")
      .master("local")
      //      .config("spark.mongodb.auth.uri", "mongodb://root:123%25abc@119.23.34.178:27017/admin")
      //      .config("spark.mongodb.input.uri", "mongodb://119.23.34.178:27017/recommend.gio_custom_event_202012")
      .getOrCreate()

    val readConfig = ReadConfig(Map("uri" -> "mongodb://root:123%25abc@119.23.34.178:27017",
      "database" -> "recommend", "collection" -> "gio_custom_event_202012"))

    val df = MongoSpark.load(spark, readConfig)
    df.createTempView("custom_event")

    val df1 = spark.sql("select loginUserId as userCode, eventVariable.commodity_no as itemCode, time from custom_event where eventName = 'click_goodsDetails'")
    //    val df1 = spark.sql("select eventName, count(1) as c  from custom_event group by eventName order by c")

    val userCodes = df1.select("userCode").distinct()
    val userIndexer = new StringIndexer()
      .setInputCol("userCode")
      .setOutputCol("userId")
    val userIndexModel = userIndexer.fit(userCodes)
    val usersDf = userIndexModel.transform(userCodes)
    usersDf.show()
    println("user count: " + usersDf.count())

    val itemCodes = df1.select("itemCode").distinct()
    val itemIndexer = new StringIndexer()
      .setInputCol("itemCode")
      .setOutputCol("itemId")
    val itemIndexModel = itemIndexer.fit(itemCodes)
    val itemsDf = itemIndexModel.transform(itemCodes)
    itemsDf.show()
    println("item count: " + itemsDf.count())

    df1.createTempView("logs")
    usersDf.createTempView("users")
    itemsDf.createTempView("items")

    val ratings = spark.sql(s"select userId, itemId, count(1) as rating from logs join users on logs.userCode = users.userCode join items on logs.itemCode = items.itemCode group by userId, itemId")
    ratings.show()
    println("ratings count: " + ratings.count())

    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    val als = new ALS()
      .setMaxIter(20)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("itemId")
      .setRatingCol("rating")
      .setImplicitPrefs(false)

    val model = als.fit(training)

    model.setColdStartStrategy("drop")
    val predictions = model.transform(test)

    predictions.show()

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)


    println(s"Root-mean-square error = $rmse")

    model.write.overwrite().save("/Users/season/Sources/Personal/spark-demo/src/main/resources/mongo/model")
    usersDf.write.parquet("/Users/season/Sources/Personal/spark-demo/src/main/resources/mongo/users.parquet")
    itemsDf.write.parquet("/Users/season/Sources/Personal/spark-demo/src/main/resources/mongo/items.parquet")

  }
}
