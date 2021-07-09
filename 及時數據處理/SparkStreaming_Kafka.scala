package cn.it.streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.codehaus.jackson.map.deser.std.StringDeserializer


object SparkStreaming_Kafka_Demo01 {
  def main(args: Array[String]): Unit = {
    //準備環境
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5)) //每5秒分一個批次!!
    ssc.checkpoint("./ckp")  // 設置checkpoint(一般都放置在HDFS)

    //加載數據
    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> "node1:9092",            //kafka集群地址
      "key.deserializer" -> classOf[StringDeserializer],      //key反序列化規則
      "value.deserializer" -> classOf[StringDeserializer],    //value反序列化規則
      "group.id" -> "sparkdemo",                              //消費者組名稱
      "auto.offset.reset" -> "latest",
      //earliest:表示如果有offset紀錄,從offset紀錄開始消費,如果沒有從最早的消息開始消費
      //latest:表示如果有offset紀錄,從offset紀錄開始消費,如果沒有從(最後,最新)的消息開始消費
      //none:表示如果有offset紀錄,從offset紀錄開始消費,如果沒有就報錯!!!
      "auto.commit.interval.ms" -> "1000",                    //自動提交的時間間隔
      "enable.auto.commit" -> (true: java.lang.Boolean)       //是否自動提交
    )
    val topics = Array("spark_kafka") //訂閱的topic
    //使用工具類從kafka中獲取消息!!!
    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent, //位子策略(使用源碼中推薦)
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams) //消費策略(使用源碼中推薦)
    )

    //處理消息
    val infoDS: DStream[String] = kafkaDS.map(recode => {
      val topic: String = recode.topic()
      val partition: Int = recode.partition()
      val offset: Long = recode.offset()
      val key: String = recode.key()
      val value: String = recode.value()
      val info: String = """topic:${topic},partition:${partition},offset:${offset},key:${key},value:${value}"""
      info
    })

    infoDS.print()

    // 啟動並等待關閉
    ssc.start()
    ssc.awaitTermination()

    // 關閉
    ssc.stop(true,true)

  }
}
