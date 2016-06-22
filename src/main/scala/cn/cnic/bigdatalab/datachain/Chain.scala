package cn.cnic.bigdatalab.datachain

import cn.cnic.bigdatalab.datachain.TransformerStep

/**
  * Created by cnic on 2016/6/21.
  */
class Chain {

  var collection:CollectionStep = _
  var transformer:TransformerStep = _
  var task:TaskStep = _

  def addStep(obj: Object): Chain = {

    this
  }


  def run()={


  }

}
