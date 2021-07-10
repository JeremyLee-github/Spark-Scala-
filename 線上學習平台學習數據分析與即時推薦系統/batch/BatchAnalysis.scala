package cn.it.edu.analysis.batch

import cn.it.edu.bean.AnswerWithRecommendations
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import java.util.Properties

//離線分析學生學習情況
object BatchAnalysis {
  def main(args: Array[String]): Unit = {
    //準備環境(SparkSession)
    val spark: SparkSession = SparkSession.builder().appName("sparksql").master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //數據加載-MySQL
    val properties: Properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","root")
    val allInfoDF: Dataset[AnswerWithRecommendations] = spark.read.jdbc(
      "jdbc:mysql://localhost:3306/edu?useUnicode=true&characterEncoding=utf8",
      "t_recommended",
      properties).as[AnswerWithRecommendations] //轉為.as[AnswerWithRecommendations]範型資料格式(最後一項帶有預測推薦題目!!!)

    //處理分析數據
    //需求一:各科目熱門題目分析(找到TOP50熱門題目,然後統計這些科目中,分別包含這幾個熱門題目的條目數)
    /*
    科目,包含熱門題目數
    數學,98
    英文,74
     */
    //TODO ===SQL
    """
      |select subject_id, count(t_answer.question_id) as hot_question_count
      |from
      |(select question_id, count(*) as frequency
      |from t_answer
      |group by question_id
      |order by frequency desc
      |limit 50) t1
      |join t_answer
      |on t1.question_id = t_answer.question_id
      |group by subject_id
      |order by hot_question_count desc
      |""".stripMargin
    //TODO ===DSL
    //1.統計TOP50熱門題目--子查詢t1
    val hotTOP50: Dataset[Row] = allInfoDF.groupBy('question_id).agg(count('question_id) as "hot_question_count")
      .orderBy('hot_question_count.desc).limit(50)
    //2.將t1和原始列表t_answer關聯,得到熱門題目對應的科目
    val joinDF: DataFrame = hotTOP50.join(allInfoDF, 'question_id)
    //3.按照科目分組聚合統計各個科目包含熱門題目的數量
    val result1: Dataset[Row] = joinDF.groupBy('subject_id).agg(count("*") as "hotCount")
      .orderBy('hotCount.desc)

    //需求二:各科目推薦題目分析 (找到TOP20熱門題目對應的推薦題目,然後找到推薦題目對應的科目,並統計每個科目分別包含推薦題目的個數)
    /*
    題號 熱度
    1    10
    2     9
    題號  熱度  推薦題
    1    10   2,3,4
    2     9   3,4,5
    推薦題   科目
    2       數學
    3       數學
    4       物理
    5       化學
    科目   推薦題目數量
    數學     5
    數學     4
    物理     3
    化學     1
     */
    //TODO ===DSL
    //1.統計TOP20熱門題目--子查詢t1
    val hotTOP20: Dataset[Row] = allInfoDF.groupBy('question_id).agg(count('question_id) as "hot_question_count")
      .orderBy('hot_question_count.desc).limit(20)
    //2.將t1和原始列表t_answer關聯,得到TOP20熱門題目的推薦列表t2
    val ridAndsDS: DataFrame = hotTOP20.join(allInfoDF.dropDuplicates("question_id"),'question_id)
      .select("recommendations")
    //3.用Split將recommendations中的ids用","切分為數組,然後用explode將列轉為行,並記為t3
    val ridsDS: Dataset[Row] = ridAndsDS.select(explode(split('recommendations, ",") as "question_id"))
      .dropDuplicates("rid")
    //4.對推薦題目進行去重,將t3和t_answer原始表進行join,得到每個推薦題目所屬的科目,記為t4
    //df1.join(df2, $"df1key"===$"df2key")  or  df1.join(df2).where($"df1key"===$"df2key") =>源碼join兩種寫法
    val ridsAndSid: DataFrame = ridsDS.join(allInfoDF.dropDuplicates("question_id"), "question_id")
    //5.統計各個科目包含的推薦題目數量並倒序排列(已經去重)
    val result2: Dataset[Row] = ridsAndSid.groupBy("subject_id")
      .agg(count("*") as "rcount").orderBy('rcount.desc)


    //輸出結果
    result1.show()
    result2.show()

    //關閉資源(因為是離線分析,所以可以直接關閉,不用等待!!)
    spark.stop()
  }
}
