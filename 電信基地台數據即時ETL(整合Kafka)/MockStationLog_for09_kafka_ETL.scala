package cn.it.structured

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

/*
模擬產生基站日誌數據,實時發送Kafka Topic
數據字段訊息 : 基站標示符號ID, 主機號碼, 被叫號碼, 通話狀態, 通話時間, 通話長度

// 查看Topic訊息
/kafka/bin/kafka-topic.sh --list --zookeeper node1:2182

// 刪除Topic (將原有的stationTopic跟etlTopic刪除)
/kafka/bin/kafka-topic.sh --delete --zookeeper node1:2181 --topic stationTopic
/kafka/bin/kafka-topic.sh --delete --zookeeper node1:2181 --topic etlTopic

// 創建Topic
/kafka/bin/kafka-topic.sh --create --zookeeper node1:2181 --topic stationTopic
/kafka/bin/kafka-topic.sh --create --zookeeper node1:2181 --topic etlTopic

// 模擬producer
/kafka/bin/kafka-console-producer.sh --broker-list node1:9092 --topic stationTopic
/kafka/bin/kafka-console-producer.sh --broker-list node1:9092 --topic etlTopic

// 模擬consumer
/kafka/bin/kafka-console-consumer.sh --bootstrap-server node1:9092 --topic stationTopic --from-beginning
/kafka/bin/kafka-console-consumer.sh --bootstrap-server node1:9092 --topic etlTopic --from-beginning
 */
object MockStationLog_for09_kafka_ETL {
  def main(args: Array[String]): Unit = {
    // 發送Kafka Topic
    val props: Properties = new Properties
    props.put("bootstrap.servers", "node1:9092")
    props.put("acks", "1")
    props.put("retries", "3")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

    val random = new Random()
    val allStatus = Array("fail", "busy", "barring", "success", "success", "success", "success", "success", "success", "success", "success", "success")

    while (true) {
      val callOut: String = "1860000%04d".format(random.nextInt(10000))
      val callIn: String = "1890000%04d".format(random.nextInt(10000))
      val callStatus: String = allStatus(random.nextInt(allStatus.length))
      val callDuration = if ("success".equals(callStatus)) ( 1+ random.nextInt(10)) * 1000L else 0L

      // 隨機產生一條基站日誌數據
      val stationLong: StationLong = StationLong(
        "station_" + random.nextInt(10),
        callOut,
        callIn,
        callStatus,
        System.currentTimeMillis(),
        callDuration
      )

      println(stationLong.toString)
      Thread.sleep(100 + random.nextInt(100))

      val record: ProducerRecord[String, String] = new ProducerRecord[String, String]("stationTopic", stationLong.toString)
      producer.send(record)
    }
    // 關閉連接
    producer.close()
  }
  /*
  基站通話日誌數據
   */
  case class StationLong (
                         stationId: String, // 基站標示符號ID
                         callOut: String,   // 主機號碼
                         callIn: String,    // 被叫號碼
                         callStatus: String,// 通話狀態
                         callTime: Long,    // 通話時間
                         duration: Long     // 通話長度
                         ) {
    override def toString: String = {s"$stationId,$callOut,$callIn,$callStatus,$callTime,$duration"}
  }
}
