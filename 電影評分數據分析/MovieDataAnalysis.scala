package cn.it.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Demo07_MovieDataAnalysis_csv {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("sparkSQL").master("local[*]")
      .config("spark.sql.shuffle.partitions","4") //spark默認分區數為200,因為在本機運行可以設置小一點
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    //加載數據
    val ds: Dataset[String] = spark.read.textFile("data/input/ratings/ratings.csv")

    val df: DataFrame = ds.map(line => {
      val arr: Array[String] = line.split(",")
      (arr(1), arr(2).toFloat)
    }).toDF("movieId", "score")

    df.printSchema()

    //統計評分>200的電影,並取其前TOP10
    //TODO =====SQL=====
    df.createOrReplaceTempView("t_movies")
    spark.sql("select movieId, avg(score) as avgscore, count(*) as counts " +
      "from t_movies " +
      "group by movieId " +
      "having counts > 200 " +
      "order by avgscore desc " +
      "limit 10 ").show()

    //TODO =====DSL=====
    import org.apache.spark.sql.functions._
    df.groupBy('movieId)
      .agg(
        avg('score) as 'avgscore,
        count('movieId) as 'count
      )
      .filter('count > 200)
      .orderBy('avgscore.desc)
      .limit(10)
      .show()

    spark.stop()

  }
}
