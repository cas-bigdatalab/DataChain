package cn.cnic.bigdatalab.Collection

/**
  * Created by xjzhu on 16/6/22.
  */

class AgentSink(sinkName:String, sinkConf:Map[String,String]){
  def getName(): String ={
    sinkName
  }

  def getConf(): Map[String, String] ={
    sinkConf
  }
}

//class HdfsSink(sinkType:String, sinkName:String) extends AgentSink(sinkType:String, sinkName:String){
//
//  var channels: String = _
//  var path: String = _
//  var filePrefix: String = _
//  var round: Boolean = _
//  var roundValue: Int = _
//  var roundUnit: String = _
//}
