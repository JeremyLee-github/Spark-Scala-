package cn.it.core

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term

import java.util

object HanLPTest {
  def main(args: Array[String]): Unit = {

    val words = "[HanLPTest入門測試]"
    val terms: util.List[Term] = HanLP.segment(words)
    println(terms) //直接打印Java的List:[[/w, HanLPTest/nx, 入/v, 門/n, 測/w, 試/n, ]/w]

    import scala.collection.JavaConverters._
    println(terms.asScala.map(_.word)) //打印Scala的List:ArrayBuffer([, HanLPTest, 入, 門, 測, 試, ])

    val cleanWords1: String = words.replaceAll("\\[|\\]", "") //將"["或"]"兩個符號,替換成""(空字符)
    println(cleanWords1)
    println(HanLP.segment(cleanWords1).asScala.map(_.word))

    val log = """00:00:00	2982199073774412	[360安全卫士]	8 3	download.it.com.cn/softweb/software/firewall/antivirus/20067/17938.html"""
    val cleanWords2 = log.split("\\s+")(2)       //[360安全卫士]
      .replaceAll("\\[|\\]","")      //360安全卫士
    println(cleanWords2)
    println(HanLP.segment(cleanWords2).asScala.map(_.word))

  }
}
