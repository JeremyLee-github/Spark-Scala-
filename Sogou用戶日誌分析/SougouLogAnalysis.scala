package cn.it.core

import com.hankcs.hanlp.HanLP
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object SougouLogAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val lines = sc.textFile("data/input/ratings/SogouQ.sample")

    // 把每一條數據封裝成SogoRecord1樣例類
    val SogoRecordRDD: RDD[SogoRecord1] = lines.map(line => {
      val arr: Array[String] = line.split("\\s+")
      SogoRecord1(
        arr(0),
        arr(1),
        arr(2),
        arr(3).toInt,
        arr(4).toInt,
        arr(5)
      )
    })
    //詞頻切割
    val wordsRDD: RDD[String] = SogoRecordRDD.flatMap(record => {
      val words: String = record.queryWord.replaceAll("\\[|\\]", "")  //將"["或"]"兩個符號,替換成""(空字符)
      import scala.collection.JavaConverters._
      HanLP.segment(words).asScala.map(_.word)  // 透過HanLP將詞分切
    })

    //TODO 統計分析
    //--1.熱門搜索關鍵字統計
    val result1: Array[(String, Int)] = wordsRDD
      .filter(word => !word.equals(".") && !word.equals("+"))  // 將數據內"."跟"+"去除!!
      .map(_ -> 1)
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(10)

    result1.foreach(println)

    println("===================================================")

    //--2.用戶熱門搜索詞(加上用戶ID進行統計)
    val userIdAndWordsRDD: RDD[(String, String)] = SogoRecordRDD.flatMap(record1 => {
      val wordsStr: String = record1.queryWord.replaceAll("\\[|\\]", "")
      import scala.collection.JavaConverters._
      val words: mutable.Buffer[String] = HanLP.segment(wordsStr).asScala.map(_.word)
      val userId: String = record1.userId
      words.map(word => (userId, word))
    })

    val result2: Array[((String, String), Int)] = userIdAndWordsRDD
      .filter(t => !t._2.equals(".") && !t._2.equals("+"))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(10)

    result2.foreach(println)

    println("=============================================================")

    //--3.各個時間段搜索熱度統計
    val result3: Array[(String, Int)] = SogoRecordRDD.map(record2 => {
      val hourAndMinStr = record2.queryTime.substring(0, 5)
      (hourAndMinStr, 1)
    }).reduceByKey(_ + _).sortBy(_._2,false).take(10)

    result3.foreach(println(_))

    println("=============================================================")

    sc.stop()
  }
  // 將數據資料封裝成一個樣例類
  case class SogoRecord1(
                       queryTime:String,  // 訪問時間(格式:HH:mm:ss)
                       userId:String,     // 用戶ID
                       queryWord:String,  // 查詢詞
                       resultRank:Int,    // 該URL在返回結果中的排名
                       clickRank:Int,     // 用戶點擊的順序
                       clickUrl:String    // 用戶點擊的URL
                       )

}
