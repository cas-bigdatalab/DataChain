package com.github.casbigdatalab.datachain.dataparser

/**
 * Created by cnic-liliang on 2016/6/3.
 */

import java.util

object customized_parserTest {
  class customized_parser(schema_json: String, mapping_json : String) extends common {

    override def parse(msg:String): util.ArrayList[Map[String, Any]] = {
      val re = "customised: " + msg
      val result = new util.ArrayList[Map[String, Any]]()
      result.add(Map("cust"->"cust"))
      result
    }
  }

  def main(agrs: Array[String]): Unit = {
    println("beginning testing cust dataparser")
    val myparser = new dataparser("mapping.json", new customized_parser("schema.json", "mapping.json"))
//    val dataparser = parser.getparser()
    val out = myparser.parse("abc")
    println(out)
  }
  //
}