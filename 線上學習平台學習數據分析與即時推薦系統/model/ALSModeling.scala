package cn.it.edu.model

import cn.it.edu.bean.{Answer, Rating}
import cn.it.edu.utils.RedisUtil
import com.google.gson.Gson
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.json4s.jackson.Json
import redis.clients.jedis.Jedis

object ALSModeling {
  def main(args: Array[String]): Unit = {
    //創建環境
    val spark: SparkSession = SparkSession.builder().master(" ALSModeling")
      .config("spark.local.dir", "temp").config("spark.sql.shuffle.partitions", "4").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //加載數據並轉換成:Dataset[Rating(學生ID,題目ID,推薦指數)]
    val path: String = "data/output/question_info.json"
    //val answerInfoDS: Dataset[Rating] = spark.read.textFile("").map(parserAnswerInfo).cache()
    val answerInfoDS: Dataset[Rating] = spark.sparkContext.textFile(path)
      .map(parserAnswerInfo).toDS().cache()

    //劃分數據
    val randomSplits: Array[Dataset[Rating]] = answerInfoDS.randomSplit(Array(0.8, 0.2),11l)

    //建立ALS-model
    val als: ALS = new ALS()
      .setRank(20)
      .setMaxIter(15)
      .setRegParam(0.09)
      .setAlpha(0.1)
      .setUserCol("student_id")
      .setItemCol("question_id")
      .setRatingCol("rating")

    //訓練模型
    val model: ALSModel = als.fit(randomSplits(0).cache()).setColdStartStrategy("drop")
    //.setColdStartStrategy("drop")=>冷啟動策略

    //使用模型做推薦
    val recommend: DataFrame = model.recommendForAllUsers(20)

    //對測試集進行預測(.transform()回來的值包含原本rating值跟預測rating值)
    val predictions: DataFrame = model.transform(randomSplits(1).cache())

    //使用RMSE進行評估
    val evaluator: RegressionEvaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    val rmse: Double = evaluator.evaluate(predictions)

    //輸出結果
    //推薦結果
    recommend.foreach(x => println("學生ID:"+x(0), "推薦題目:"+x(1)))
    //預測結果
    predictions.foreach(x=>println("預測結果"+x))
    //輸出誤差值RMSE
    println("RMSE:"+rmse)

    //將訓練好模型保存到Redis
    val jedis: Jedis = RedisUtil.pool.getResource
    //jedis.select(1)
    if (rmse<=1.5){
      val path: String = "data/output/als_model" + System.currentTimeMillis()
      model.save(path)
      jedis.hset("als_model","recommended_question_id", path)
      println("模型PATH已經保存至redis!!!")
    }

    //關閉釋放資源
    answerInfoDS.unpersist()
    randomSplits(0).unpersist()
    randomSplits(1).unpersist()
    RedisUtil.pool.returnResource(jedis)

  }
  /*
將學生答題的詳細訊息轉為Rating(學生ID,題目ID,推薦指數)
*/
  def parserAnswerInfo(json: String):Rating ={
    //1.獲取學生答題訊息(學生ID,題目ID,推薦指數)
    val gson: Gson = new Gson()
    val answer: Answer = gson.fromJson(json, classOf[Answer])
    val studentID:Long = answer.student_id.split("_")(1).toLong    //學生ID
    val questionID: Long = answer.question_is.split("_")(1).toLong //題目ID
    val rating: Int = answer.score  //評分

    //2.計算推薦指數:(得分越低的題目,推薦指數越高)
    val ratingFix:Int =
      if(rating <= 3) 3
      else if(rating > 3 && rating <= 8) 2
      else 1

    //返回(學生ID,題目ID,推薦指數)
    Rating(studentID, questionID, ratingFix)
  }
}

