package cn.edana.spark.cf

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

object ALSExample {

  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ALSExample")
      .master("local")
      .getOrCreate();

    import spark.implicits._

    val path = "/Users/season/Develop/tools/spark/data/mllib/als/sample_movielens_ratings.txt"
    val ratings = spark.read.textFile(path)
      .map(parseRating)
      .toDF()
    ratings.show()

    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

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

    val userRecs = model.recommendForAllUsers(10)
    val movieRecs = model.recommendForAllItems(10)
    val users = ratings.select(als.getUserCol).distinct().limit(3)
    val userSubsetRecs = model.recommendForUserSubset(users , 10)
    val movies = ratings.select(als.getItemCol).distinct().limit(3)
    val movieSubsetRecs = model.recommendForItemSubset(movies, 10)

    userRecs.show()
    movieRecs.show()

    userSubsetRecs.show()
    movieSubsetRecs.show()

  }

}
