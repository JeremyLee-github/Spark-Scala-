package cn.it.structured

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{avg, count, get_json_object}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.codehaus.commons.compiler.util.StringUtil

object Demo10_Kafka_IOT {
  def main(args: Array[String]): Unit = {
    //創建環境
    //因為StructuredStreaming是基於SparkSQL且編程API/數據是DataFrame/Dataset,所以這裡是創建SparkSession環境
    val spark: SparkSession = SparkSession.builder().appName("sparksql").master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //加載數據 kafka->stationTopic
    val kafkadf: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092")
      .option("subscribe", "iotTopic")
      .load()

    val valueDS: Dataset[String] = kafkadf.selectExpr("CAST(value AS STRING)") //這裡取出來的值為Boolean類型,須轉為String(官網教的)
      .as[String] //再把他轉為DS類型方便號面切割處理~~~

    //處理數據 IOT->過濾出結果
    // 先解析Json數據(這裡使用SparkSQL內痔函數處理,也可以使用fastJson/Gson等工具包)
    val schemaDF: DataFrame = valueDS.filter(StringUtils.isNotBlank(_)) //先過濾空值(import org.apache.commons.lang3.StringUtils)
      .select(  // SparkSQL官網指定寫法
        get_json_object($"value", "$.device").as("device_id"),
        get_json_object($"value", "$.deviceType").as("deviceType"),
        get_json_object($"value", "$.signal").cast(DoubleType).as("signal")  // 因為源數據signal為Double類型,這裡需要轉為.cast(DoubleType)
      )

    //TODO ====SQL
    schemaDF.createOrReplaceTempView("t_iot")
    val margin: String =
      """
        |select deviceType, count(*) as counts, avg(signal) as avgsignal
        |from t_iot
        |where signal > 30
        |group by deviceType
        |""".stripMargin
    val rdf1: DataFrame = spark.sql(margin)

    //TODO ====DSL
    val rdf2: DataFrame = schemaDF.filter('signal > 30)
      .groupBy('deviceType)
      .agg(
        count('device_id) as "counts",
        avg('signal) as "avgsignal"
      )

    //輸出結果 kafka->ETLTopic
    rdf1.writeStream.format("console").outputMode("complete").start()

    rdf2.writeStream.format("console").outputMode("complete").start().awaitTermination()

    //關閉資源
    spark.stop()

  }
}
