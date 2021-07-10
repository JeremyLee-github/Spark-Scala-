package cn.it.edu.analysis.streaming

import cn.it.edu.bean.Answer
import cn.it.edu.utils.RedisUtil
import com.google.gson.Gson
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.{SparkContext, streaming}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.codehaus.jackson.map.deser.std.StringDeserializer
import redis.clients.jedis.Jedis

import java.util.Properties

object StreamingRecommend {
  def main(args: Array[String]): Unit = {
    //準備環境
    val spark: SparkSession = SparkSession.builder().appName("spark").master("local[*]")
      .config("spark.sql.shuffle.partitions", "4").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5)) //每5秒分一個批次!!
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //加載數據
    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> "node1:9092",            //kafka集群地址
      "key.deserializer" -> classOf[StringDeserializer],      //key反序列化規則
      "value.deserializer" -> classOf[StringDeserializer],    //value反序列化規則
      "group.id" -> "StreamingRecommend",                              //消費者組名稱
      "auto.offset.reset" -> "latest",
      //earliest:表示如果有offset紀錄,從offset紀錄開始消費,如果沒有從最早的消息開始消費
      //latest:表示如果有offset紀錄,從offset紀錄開始消費,如果沒有從(最後,最新)的消息開始消費
      //none:表示如果有offset紀錄,從offset紀錄開始消費,如果沒有就報錯!!!
      "auto.commit.interval.ms" -> "1000",                    //自動提交的時間間隔
      "enable.auto.commit" -> (true: java.lang.Boolean)       //是否自動提交
    )
    val topics = Array("edu") //訂閱的topic
    //使用工具類從kafka中獲取消息!!!
    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent, //位子策略(使用源碼中推薦)
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams) //消費策略(使用源碼中推薦)
    )

    //處理數據
    val valueDStream: DStream[String] = kafkaDS.map(record => {
      record.value()
    })
    valueDStream.foreachRDD(rdd=>{
      if(!rdd.isEmpty()){
        //該rdd表示每次微批獲取的數據
        //取的redis連接
        val jedis: Jedis = RedisUtil.pool.getResource
        //加載模型路徑
        //jedis.hset("als_model", "recommended_question_id", path)
        val path: String = jedis.hget("als_model", "recommended_question_id")
        //根據路徑加載模型
        val model: ALSModel = ALSModel.load(path)
        //取出用戶ID(使用new Gson()套入Answer樣例類進行轉換!!!)
        val answerDF: DataFrame = rdd.coalesce(1).map(jsonStr => {
          val gson: Gson = new Gson()
          gson.fromJson(jsonStr, classOf[Answer])
        }).toDF()
        //將用戶名稱作切分取出ID號
        val id2int = udf((student_id:String)=>{
          student_id.split("_")(1).toInt
        })
        val studentIdDF: DataFrame = answerDF.select(id2int('student_id) as "student_id")
        //使用模型做預測
        val recommendDF: DataFrame = model.recommendForUserSubset(studentIdDF, 10)
        recommendDF.printSchema()
        recommendDF.show(false)
        //處理推薦數據
        val recommendResultDF: DataFrame = recommendDF.as[(Int, Array[(Int, Float)])].map(t => {
          val studentIDstr: String = "學生ID_" + t._1
          val questionIDstr: String = t._2.map(_._2).mkString(",") //mkString(",")=>將字符用","拼接
          (studentIDstr, questionIDstr)
        }).toDF("student_id", "recommendations")
        //將answerDF和recommendResultDF進行連接
        val allInfoDF: DataFrame = answerDF.join(recommendDF, "student_id")  //使用"student_id"當作keyID進行連接!!!
        //將數據儲存到MySQL/HBase中
        if(allInfoDF.count()>0){
          val properties: Properties = new Properties()
          properties.setProperty("user","root")
          properties.setProperty("password","root")
          allInfoDF.write.mode(SaveMode.Append).jdbc(
            "jdbc:mysql://localhost:3306/edu?useUnicode=true&characterEncoding=utf8",
            "t_recommended", properties)
        }
        //關閉redis連接
        jedis.close()
      }
    })
    //啟動並等待結束
    ssc.start()
    ssc.awaitTermination() //注意:流式應用程序啟動之後需要一直運行等待手動停止OR等待數據到來

    //關閉資源
    ssc.stop(stopSparkContext = true, stopGracefully = true) //優雅關閉!!

  }
}
