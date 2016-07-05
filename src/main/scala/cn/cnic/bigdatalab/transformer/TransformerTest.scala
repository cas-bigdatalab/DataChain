/**
 * Created by cnic-liliang on 2016/6/3.
 */

import java.util.regex.Pattern
import scala.collection.mutable.ArrayBuffer
import cn.cnic.bigdatalab.transformer.{TMapping, Transformer}


object TransformerTest {
  def main(agrs: Array[String]): Unit = {
    println("beginning testing dataparser")
    val home_dir = "E:\\bigdatalab\\DataChain"

    var mapping_conf = home_dir + "\\conf\\" + "csvMapping.json"
    //csv msg
    println("----------------csvMapping Test-------------------")
    var msg = "1996,Jeep,Grand Cherokee,\"MUST SELL! air, moon roof, loaded\", 4799.00"
    var csvparser = new Transformer(mapping_conf)
    var out = csvparser.transform(msg)
    println("msg: " + msg)
    println("parse result: " + out)

    //json
    println("----------------jsonMapping Test------------------")
    msg = "{\"year\": \"1996\", \"make\": \"jeep\", \"model\": \"Grand Cherokee\", \"comment\": \"MUST SELL! air, moon roof, loaded\", \"blank\": \"4799\"}"//
    mapping_conf = home_dir + "\\conf\\" + "jsonMapping.json"
    val jsonparser = new Transformer(mapping_conf)
    println("msg: " + msg)
    println("parse result: " + jsonparser.transform((msg)))

    println("---------------regexMapping Test------------------")
    println("combined Apache log")
    msg = "127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326 \"http://www.example.com/start.html\" \"Mozilla/4.08 [en] (Win98; I ;Nav)\""
    mapping_conf = home_dir + "\\conf\\" + "combinedApacheLogMapping.json"
    var regexparser = new Transformer(mapping_conf)
    println("msg: " + msg)
    println("parse result: " + regexparser.transform((msg)))

    println("common Apache log")
    msg = "127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326"
    mapping_conf = home_dir + "\\conf\\" + "commonApacheLogMapping.json"
    regexparser = new Transformer(mapping_conf)
    println("msg: " + msg)
    println("parse result: " + regexparser.transform((msg)))

    println("log4j log")
    msg = "1999-11-27 15:49:37,459 [thread-x] ERROR mypackage - Catastrophic system failure"
    msg = "2013-12-25 19:57:06,954 [10.207.37.161] WARN impl.PermanentTairDaoImpl - Fail to Read Permanent Tair,key:e:470217319319741_1,result:com.example.tair.Result@172e3ebc[rc=code=-1, msg=connection error or timeout,value=,flag=0]"
    mapping_conf = home_dir + "\\conf\\" + "log4jMapping.json"
    regexparser = new Transformer(mapping_conf)
    println("msg: " + msg)
    println("parse result: " + regexparser.transform((msg)))

    println("python log")
    msg = "2016-02-19 11:06:52,514 - test.py:19 - 10 DEBUG test.py test <module> 1455851212.514271 139865996687072 MainThread 20193 tst - first debug message"
    mapping_conf = home_dir + "\\conf\\" + "pythonLogMapping.json"
    regexparser = new Transformer(mapping_conf)
    println("msg: " + msg)
    println("parse result: " + regexparser.transform(msg))
    println("getSchema: " + regexparser.getSchema())

    //Mapping test
    val map = new TMapping(mapping_conf)
    regexparser = new Transformer(map)
    println("transform result " + regexparser.transform(msg))

    //morphlines
    //json
    var morphlineFile = "E:\\bigdatalab\\DataChain" + "\\conf\\" + "morphlinesMapping.json"
    var mortransformer = new Transformer(morphlineFile)
    var mymsg = ""
    mymsg = "{\"year\": \"1996\", \"make\": \"jeep\", \"model\": \"Grand Cherokee\", \"comment\": \"MUST SELL! air, moon roof, loaded\", \"blank\": \"4799\"}"//
    println("msg: " + mymsg)
    println("morphline parse result: " + mortransformer.transform(mymsg))
    println("morphline schema: " +  mortransformer.getSchema())

    //csv+tmap
    morphlineFile = "E:\\bigdatalab\\DataChain" + "\\conf\\" + "morphlinesCSVMapping.json"
    mymsg = "13457, Coral, Seatttle, 12545"//
    var dim = new ArrayBuffer[String]()
    val strArray = "id:int,name:string,city:string,number:int".split(",")
    for(item <- strArray) dim += item.toString
    val tmap = new TMapping()
            .setMapType("morphlinesMapping")
            .setConf( "E:\\bigdatalab\\DataChain\\conf\\morphlines.csf.conf")
            .setDimensions(dim)
    mortransformer = new Transformer(tmap)
    println("msg: " + mymsg)
    println("morphline csv result: " + mortransformer.transform(mymsg))
    println("morphline csv schema: " +  mortransformer.getSchema())

  }
}