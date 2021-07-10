package cn.it.edu.model

import org.apache.spark.SparkContext
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object ALSMovieDemoTest {
  def main(args: Array[String]): Unit = {
    //準備環境(SparkSession)
    val spark: SparkSession = SparkSession.builder().appName("sparksql").master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //加載數據
    val fileDS: Dataset[String] = spark.read.textFile("data/input/")
    val ratingDF: DataFrame = fileDS.map(line => {
      val arr: Array[String] = line.split("\t")
      (arr(0).toInt, arr(1).toInt, arr(2).toFloat)
    }).toDF("userID", "movieID", "score") //在這裡把DS轉DF,方便後面ALS-model使用col代入

    //切分數據(train_set:test_set => 0.8:0.2)
    val Array(trainSet,testSet): Array[Dataset[Row]] = ratingDF.randomSplit(Array(0.8, 0.2))

    //建立ALS-model
    val als: ALS = new ALS()
      .setUserCol("userID") //設置用戶ID欄位
      .setItemCol("movieID") //設置產品ID欄位
      .setRatingCol("score") //設置評分欄位
      .setRank(10) //可以理解 Cm*n = Am*k X Bk*n 裡面的k值
      .setMaxIter(10) //設置最大疊代次數
      .setAlpha(1.0) //設置步長learn_rate

    //訓練模型
    val model: ALSModel = als.fit(trainSet)

    //使用model預測推薦數據
    val result1: DataFrame = model.recommendForAllUsers(5) // 給所有用戶各推薦5部電影
    val result2: DataFrame = model.recommendForAllItems(5) // 給所有電影各推薦5個用戶
    //給指定用戶(196)推薦5部電影
    val result3: DataFrame = model.recommendForUserSubset(sc.makeRDD(Array(196)).toDF("userID"), 5)
    //給指定電影(242)推薦5個用戶
    val result4: DataFrame = model.recommendForItemSubset(sc.makeRDD(Array(242)).toDF("movieID"), 5)

    //輸出結果
    result1.show()
    result2.show()
    result3.show()
    result4.show()

  }
}
