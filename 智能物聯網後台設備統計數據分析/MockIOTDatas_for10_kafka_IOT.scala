package cn.it.structured

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.json4s.jackson.Json

import java.util.Properties
import scala.util.Random

/*
模擬產生基站日誌數據,實時發送Kafka Topic
數據字段訊息 : 基站標示符號ID, 主機號碼, 被叫號碼, 通話狀態, 通話時間, 通話長度

// 查看Topic訊息
/kafka/bin/kafka-topic.sh --list --zookeeper node1:2182

// 刪除Topic (將原有的stationTopic跟etlTopic刪除)
/kafka/bin/kafka-topic.sh --delete --zookeeper node1:2181 --topic iotTopic

// 創建Topic
/kafka/bin/kafka-topic.sh --create --zookeeper node1:2181 --topic iotTopic

// 模擬consumer
/kafka/bin/kafka-console-consumer.sh --bootstrap-server node1:9092 --topic iotTopic --from-beginning

 */
object MockIOTDatas_for10_kafka_IOT {
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
    val deviceTypes = Array("db","bigdata","kafka","route","bigdata","db","bigdata","bigdata","bigdata","bigdata")

    while (true) {

      val index: Int = random.nextInt(deviceTypes.length)
      val deviceId: String = s"device_${(index + 1) * 10 + random.nextInt(index + 1)}"
      val deviceType: String = deviceTypes(index)
      val deviceSignal: Int = 10 + random.nextInt(90)
      // 模擬構造設備數據
      val deviceData: DeviceData = DeviceData(deviceId, deviceType, deviceSignal, System.currentTimeMillis())
      // 轉換成Json
      val deviceJson: String = new Json(org.json4s.DefaultFormats).write(deviceData)

      println(deviceJson)
      Thread.sleep(100 + random.nextInt(500))

      val record: ProducerRecord[String, String] = new ProducerRecord[String, String]("iotTopic", deviceJson)
      producer.send(record)
    }
    // 關閉連接
    producer.close()
  }
  /*
  物聯網設備發送狀態數據
   */
  case class DeviceData (
                        device: String,     // 設備ID
                        deviceType: String, // 設備類型(mysql,redis,kafka,route)
                        signal: Double,     // 設備訊號
                        time: Long          // 數據發送時間
                        )
}
