/**
 * Created by cnic-liliang on 2016/6/3.
 */

import com.github.casbigdatalab.datachain.dataparser.dataparser

object dataparserTest {
  def main(agrs: Array[String]): Unit = {
    println("beginning testing dataparser")
    val home_dir = "E:\\bigdatalab\\DataChain"

    var mapping_conf = home_dir + "\\conf\\" + "csvMapping.json"
    //csv msg
    println("----------------csvMapping Test-------------------")
    var msg = "1996,Jeep,Grand Cherokee,\"MUST SELL! air, moon roof, loaded\", 4799.00"
    var csvparser = new dataparser(mapping_conf)
    var out = csvparser.parse(msg)
    println("msg: " + msg)
    println("parse result: " + out)

    //json
    println("----------------jsonMapping Test------------------")
    msg = "{\"year\": \"1996\", \"make\": \"jeep\", \"model\": \"Grand Cherokee\", \"comment\": \"MUST SELL! air, moon roof, loaded\", \"blank\": \"4799\"}"//
    mapping_conf = home_dir + "\\conf\\" + "jsonMapping.json"
    val jsonparser = new dataparser(mapping_conf)
    println("msg: " + msg)
    println("parse result: : " + jsonparser.parse((msg)))

    println("---------------regexMapping Test------------------")
    msg = "127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326 \"http://www.example.com/start.html\" \"Mozilla/4.08 [en] (Win98; I ;Nav)\""
    mapping_conf = home_dir + "\\conf\\" + "regexMapping.json"
    var regexparser = new dataparser(mapping_conf)
    println("msg: " + msg)
    println("parse result: : " + regexparser.parse((msg)))

  }
}