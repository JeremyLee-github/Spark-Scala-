package cn.it.edu.bean

import org.apache.arrow.flatbuf.Timestamp

case class Answer(
                 student_id:String, //學生ID
                 textbook_id:String,//教材ID
                 grade_id:String,   //年級ID
                 subject_id:String, //科目ID
                 chapter_id:String, //章節ID
                 question_is:String,//題目ID
                 score:Int,      //題目得分(0~10分)
                 answer_time:String,//答題提交時間(yyyy-MM-dd HH:mm:ss),StringType
                 ts:Timestamp       //答題提交時間,時間搓格式
                 ) extends Serializable
