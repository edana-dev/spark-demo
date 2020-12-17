package cn.edana.spark.gbdtlr

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.sql.SparkSession

object GBDT {
  def main(args: Array[String]): Unit = {
    val path = "/Users/season/Sources/Personal/spark-demo/src/main/resources/processed"

    val session = SparkSession.builder()
      .appName("spark-gbdt-lr")
      .master("local")
      .getOrCreate()

    val df = session.read
      .load(path)
      .rdd
      .map(row => LabeledPoint(row.getAs[Double](0), row.getAs[SparseVector](1)))

    val boostingStrategy = BoostingStrategy.defaultParams("classification")
    boostingStrategy.numIterations = 10
    boostingStrategy.treeStrategy.numClasses = 2
    boostingStrategy.treeStrategy.maxDepth = 3
    boostingStrategy.learningRate = 0.3

//    val model = GradientBoostedTrees.train(df, boostingStrategy)

  }
}
