package cn.cnic.bigdatalab.datachain

import cn.cnic.bigdatalab.Collection.{AgentSink, AgentSource}
import org.apache.flume.{Source,Sink}

/**
  * Created by cnic on 2016/6/21.
  */
class CollectionStep {

  var source:AgentSource = _
  var sink:AgentSink = _

  def setSource(src:AgentSource): CollectionStep ={

    source = src
    this
  }

  def setSink(sk:AgentSink): CollectionStep ={
    sink = sk
    this
  }

}
