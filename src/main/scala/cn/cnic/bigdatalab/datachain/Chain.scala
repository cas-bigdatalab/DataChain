package cn.cnic.bigdatalab.datachain

import cn.cnic.bigdatalab.datachain.TransformerStep

/**
  * Created by cnic on 2016/6/21.
  */
class Chain {

  var collectionStep:CollectionStep = _
  var transformerStep:TransformerStep = _
  var taskStep:TaskStep = _

  def addStep(obj: Object): Chain = {

    this
  }


  def run()={

  }

}
