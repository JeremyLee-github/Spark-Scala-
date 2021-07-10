package cn.it.edu.bean

import org.apache.arrow.flatbuf.Timestamp

case class Rating(
                 student_id:Long,  //學生ID
                 question_is:Long, //題目ID
                 rating:Float      //推薦指數
                 ) extends Serializable
