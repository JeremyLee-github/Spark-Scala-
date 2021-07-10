package cn.it.structured

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Demo09_Kafka_ETL {
  def main(args: Array[String]): Unit = {
    //創建環境
    //因為StructuredStreaming是基於SparkSQL且編程API/數據是DataFrame/Dataset,所以這裡是創建SparkSession環境
    val spark: SparkSession = SparkSession.builder().appName("sparksql").master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    //加載數據 kafka->stationTopic
    val kafkadf: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092")
      .option("subscribe", "stationTopic")
      .load()

    val valueDS: Dataset[String] = kafkadf.selectExpr("CAST(value AS STRING)") //這裡取出來的值為Boolean類型,須轉為String(官網教的)
      .as[String] //再把DF轉為DS類型方便號面切割處理~~~

    //處理數據 ETL->過濾出Success的結果
    val etlResult: Dataset[String] = valueDS.filter(_.contains("success")) //只保留有"success"的訊息

    //輸出結果 kafka->ETLTopic
    etlResult.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092")
      .option("topic", "etlTopic")   //寫入用"topic"(跟上面讀取"subscribe"不一樣!!!),儲存到"etlTopic"
      .option("checkpointLocation","./ckp")  //跑kafka需要設定checkpoint保持數據一致姓
      .start()
      .awaitTermination()

    //關閉資源
    spark.stop()

  }
}
