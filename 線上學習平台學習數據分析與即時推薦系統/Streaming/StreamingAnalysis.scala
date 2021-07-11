package cn.it.edu.analysis.streaming

import breeze.linalg.Vector.castFunc
import breeze.linalg.min
import cn.it.edu.bean.Answer
import com.google.gson.Gson
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{count, first, get_json_object}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}

//及時重kafka的edu主題消費數據,並做及時統計分析,將結果打印到控制台
object StreamingAnalysis {
  def main(args: Array[String]): Unit = {
    //準備環境
    val spark: SparkSession = SparkSession.builder().appName("sparksql").master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    //加載數據(StructuredStreaming連接Kafka)
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092")
      .option("subscribe", "edu")
      .load()
    val kafkaDS: Dataset[String] = kafkaDF.selectExpr("CAST (value as STRING)").as[String]

    //處理數據
    //方式一:
    val schemaDF: DataFrame = kafkaDS.as[String]
      .filter(StringUtils.isNotBlank(_))
      .select(
        get_json_object($"value", "$.eventTime").as("eventTime"),
        get_json_object($"value", "$.eventType").as("eventType"),
        get_json_object($"value", "$.userID").as("userID")
      )
    //方式二(將每一條json字符串解析為一個樣例對象)
    val answerDS: Dataset[Answer] = kafkaDS.map(jsonStr => {
      val gson: Gson = new Gson() //因為kafka數據是由new Gson()5轉json,所以這裡要用new Gson()搭配Answer樣例對象進行轉換!!
      gson.fromJson(jsonStr, classOf[Answer])
    })

    //即時分析
    //1.及時統計TOP10熱點題
    """select question_id, COUNT(1) as frequency
      |from t_answer
      |group by  question_id
      |order by frequency desc
      |limit 10
      |""".stripMargin
    //TODO ===DSL
    val result1: Dataset[Row] = answerDS
      .groupBy('question_id)
      //.agg(count('question_id) as "count") //這裡也可以使用agg方式,這樣可以起別名
      .count()
      .orderBy('count.desc)
      .limit(10)

    //2.及時統計答題最活耀的年級前TOP10
    """
      |select grade_id, count(1) as frequency
      |from t_answer
      |group by grade_id
      |order by frequency desc
      |limit 10
      |""".stripMargin
    //TODO ===DSL
    val result2: Dataset[Row] = answerDS.groupBy("grade_id").count().orderBy('count.desc).limit(10)

    //3.及時統計TOP10熱點題,並帶上所屬科目的
    //SQL:select+group的語句下,後面字段為分組或聚合
    """
      |select question_id ,first(subject_id), count(1) as frequency
      |from t_answer
      |group by question_id
      |order by frequency desc
      |limit 10
      |""".stripMargin
    //TODO ===DSL
    val result3: Dataset[Row] = answerDS.groupBy("question_id")
      .agg(first('subject_id) as "subject_id", count('question_id) as "question_id")
      .orderBy('count.desc).limit(10)

    //4.及時統計每個學生得分最低的TOP10題目,並帶上哪的題目
    """
      |select student_id, first(question_id), min(score)
      |from t_answer
      |group by student_id
      |order by score
      |limit 10
      |""".stripMargin
    //TODO ===DSL
    val result4: Dataset[Row] = answerDS.groupBy("student_id")
      .agg(functions.min('score) as "minscore",first('question_id))
      .orderBy('minscore.desc).limit(10)


    //輸出結果
    //啟動並等待結束
    result1.writeStream.format("console").outputMode(outputMode = "complete").start()
    result2.writeStream.format("console").outputMode(outputMode = "complete").start()
    result3.writeStream.format("console").outputMode(outputMode = "complete").start()
    result4.writeStream.format("console").outputMode(outputMode = "complete").start().awaitTermination()

    //關閉資源
    spark.stop()
  }
}
