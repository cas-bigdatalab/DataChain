/**
 * Created by cnic-liliang on 2016/6/3.
 */

import java.util.regex.Pattern

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
    println("parse result: " + jsonparser.parse((msg)))

    println("---------------regexMapping Test------------------")
    println("combined Apache log")
    msg = "127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326 \"http://www.example.com/start.html\" \"Mozilla/4.08 [en] (Win98; I ;Nav)\""
    mapping_conf = home_dir + "\\conf\\" + "combinedApacheLogMapping.json"
    var regexparser = new dataparser(mapping_conf)
    println("msg: " + msg)
    println("parse result: " + regexparser.parse((msg)))

    println("common Apache log")
    msg = "127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326"
    mapping_conf = home_dir + "\\conf\\" + "commonApacheLogMapping.json"
    regexparser = new dataparser(mapping_conf)
    println("msg: " + msg)
    println("parse result: " + regexparser.parse((msg)))

    println("log4j log")
    msg = "1999-11-27 15:49:37,459 [thread-x] ERROR mypackage - Catastrophic system failure"
    msg = "2013-12-25 19:57:06,954 [10.207.37.161] WARN impl.PermanentTairDaoImpl - Fail to Read Permanent Tair,key:e:470217319319741_1,result:com.example.tair.Result@172e3ebc[rc=code=-1, msg=connection error or timeout,value=,flag=0]"
    mapping_conf = home_dir + "\\conf\\" + "log4jMapping.json"
    regexparser = new dataparser(mapping_conf)
    println("msg: " + msg)
    println("parse result: " + regexparser.parse((msg)))

    println("python log")
    msg = "2016-02-19 11:06:52,514 - test.py:19 - 10 DEBUG test.py test <module> 1455851212.514271 139865996687072 MainThread 20193 tst - first debug message"
    mapping_conf = home_dir + "\\conf\\" + "pythonLogMapping.json"
    regexparser = new dataparser(mapping_conf)
    println("msg: " + msg)
    println("parse result: " + regexparser.parse(msg))

    val patternstr = "(\\d+-\\d+-\\d+\\s\\S+)\\s-\\s([^:]+):(\\d+)\\s+-\\s+(\\d+)\\s+(\\w+)\\s+(\\S+)\\s+(\\w+)\\s+(\\S+)\\s+(\\S+)\\s+(\\d+)\\s+(\\w+)\\s+(\\d+)\\s+(\\w+)\\s+-\\s+(.*)"
    val pattern = Pattern.compile(patternstr)
    val m = pattern.matcher(msg)
    println(m.groupCount())
  }
}