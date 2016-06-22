package cn.cnic.bigdatalab.Collection

import org.apache.flume.source.DefaultSourceFactory
import org.apache.flume.{Context, Source}

/**
  * Created by xjzhu on 16/6/22.
  */

class AgentSource(sourceType:String, sourceName:String){

}

class SpoolDirSource(sourceType:String, sourceName:String) extends AgentSource(sourceType:String, sourceName:String){

  var channels: String = _
  val spooldir: String = _
  var fileHeader: String = _


}
