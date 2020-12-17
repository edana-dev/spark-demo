package cn.edana.spark.gbdtlr

import java.util

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._

object Prepare {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder()
      .appName("spark-gbdt-lr")
      .master("local")
      .getOrCreate()

    val path = "/Users/season/Sources/Personal/spark-demo/src/main/resources/"

    val manualSchema = StructType(Array(
      StructField("age", IntegerType, true),
      StructField("workclass", StringType, true),
      StructField("fnlwgt", IntegerType, true),
      StructField("education", StringType, true),
      StructField("education-num", IntegerType, true),
      StructField("marital-status", StringType, true),
      StructField("occupation", StringType, true),
      StructField("relationship", StringType, true),
      StructField("race", StringType, true),
      StructField("sex", StringType, true),
      StructField("capital-gain", IntegerType, true),
      StructField("capital-loss", IntegerType, true),
      StructField("hours-per-week", IntegerType, true),
      StructField("native-country", StringType, true),
      StructField("label", StringType, true)))


    val df = session.read
      .option("header", false)
      .option("ignoreLeadingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", true)
      .option("delimiter", ",")
      .option("nullValue", "?")
      .schema(manualSchema)
      .format("csv")
      .load(path + "adult.data")
    //      .limit(1000)

    df.show()

    var df1 = df
      .drop("fnlwgt")
      .na.drop()

    df1.show()

    val allFeatures = df1.columns.dropRight(1)

    val colIdx = new util.HashMap[String, Int](allFeatures.length);
    var idx = 0;
    while (idx < allFeatures.length) {
      colIdx.put(allFeatures(idx), idx)
      idx += 1
    }

    val numCols = Array("age", "education-num", "capital-gain", "capital-loss", "hours-per-week")
    val catCols = df1.columns.dropRight(1).diff(numCols)

    val numLen = numCols.length;
    val catLen = catCols.length;

//    val labelIndexer = when(col("label") === "<=50K", 0).otherwise(1);
    val labelIndexer = udf(labeludf(_:String):Int)

    df1 = df1.withColumn("indexed_label", labelIndexer(col("label"))).drop("label")
    df1.show()

    val indexerMap = new util.HashMap[String, util.HashMap[String, Int]](catCols.length)

    var i = numCols.length
    for (column <- catCols) {
      val uniqueElem = df1.select(column)
        .groupBy(column)
        .agg(column -> "count")
        .select(column)
        .rdd.map(_.getAs[String](0))
        .collect()

      val len = uniqueElem.length
      var index = 0

      val freqMap = new util.HashMap[String, Int](len)

      while (index < len) {
        freqMap.put(uniqueElem(index), i)
        index += 1
        i += 1
      }
      indexerMap.put(column, freqMap)
    }

    val bcMap = session.sparkContext.broadcast(indexerMap)
    val d = i

    val df2 = df1.rdd.map(row => {
      val indices = new Array[Int](numLen + catLen)
      val value = new Array[Double](numLen + catLen)
      var i = 0
      for (col <- numCols) {
        indices(i) = i
        value(i) = row.getAs[Int](colIdx.get(col)).toDouble
        i += 1
      }

      for (col <- catCols) {
        indices(i) = bcMap.value.get(col).get(row.getAs[String](colIdx.get(col)))
        value(i) = i
        i += 1
      }

      LabeledPoint(row.getAs[Int](numLen + catLen), new SparseVector(d, indices, value))
    })

    import session.implicits._
    val ds = df2.toDF("label", "feature")
    ds.write.save(path + "processed")
  }

  def labeludf(elem: String): Int = {
    if (elem == "<=50K")
      0
    else
      1
  }
}
