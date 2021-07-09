package cn.it.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}

object WordCount06 {
  def main(args: Array[String]): Unit = {
    //自定義輸出到指定空間!!!!!!!!!!!!
    //每隔5秒計算前10秒的數據並打印前TOP3數據!!
    //此代碼只能做當前數據計算(不能做歷史數據累加!!!)
    //準備環境
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5)) //每5秒分一個批次!!

    //加載數據
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node1", 9999)

    //處理數據
    val resultDS: DStream[(String, Int)] = lines
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKeyAndWindow((x:Int,y:Int)=>x+y,Seconds(10),Seconds(5))  //這裡Seconds()可以改成Minutes()


    val sortedResultDS: DStream[(String, Int)] = resultDS.transform(rdd => {
      val sortRDD: RDD[(String, Int)] = rdd.sortBy(_._2)
      val top3: Array[(String, Int)] = sortRDD.take(3)
      println("=====TOP3=====")
      top3.foreach(println) //因為返回為DStream類型,沒有top3排序!!所以只能在這裡輸出打印!!
      println("=====TOP3=====")
      sortRDD
    })

    //輸出結果
    sortedResultDS.print()

    //自定義輸出
    sortedResultDS.foreachRDD((rdd,time)=>{
      val milliseconds: Long = time.milliseconds
      println("===========自定義輸出=============")
      println("batchtime"+milliseconds)
      println("===========自定義輸出=============")
      //輸出到控制台
      rdd.foreach(println)
      //自定義輸出到指定路徑:HDFS
      rdd.coalesce(1).saveAsTextFile("./data/output/"+milliseconds)
      //輸出到MySQL
      rdd.foreachPartition(iter=>{
        //開啟連接
        val conn: Connection = DriverManager.getConnection("", "root", "root")
        val sql = "INSERT INTO　’ｔ_hotwords’ ('time','word','count') VALUES (?,?,?);"
        val ps: PreparedStatement = conn.prepareStatement(sql)

        iter.foreach(t=>{
          val word: String = t._1
          val count: Int = t._2
          ps.setTimestamp(1,new Timestamp(milliseconds))
          ps.setString(2,word)
          ps.setInt(3,count)
          ps.addBatch()
        })
        ps.executeBatch()
        //關閉連接
        if(conn!=null) conn.close()
        if(ps!=null) ps.close()
      })

    })

    //啟動並等待結束
    ssc.start()
    ssc.awaitTermination() //注意:流式應用程序啟動之後需要一直運行等待手動停止OR等待數據到來

    //關閉資源
    ssc.stop(stopSparkContext = true, stopGracefully = true) //優雅關閉!!
  }
}
