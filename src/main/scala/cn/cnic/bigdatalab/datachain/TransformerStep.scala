package cn.cnic.bigdatalab.datachain

import cn.cnic.bigdatalab.transformer.{Transformer, Mapping}


/**
  * Created by cnic on 2016/6/21.
  */
class TransformerStep extends Step{

  var transformer:Transformer = _

  def getTransformer():Transformer ={
    assert(transformer!=null)
    transformer
  }

  def setTransformer(mapping:Mapping):TransformerStep ={

    if (transformer == null){
      assert(mapping!=null)
      transformer = new Transformer(mapping.toString)
    }

    this
  }

  override def run: Unit = ???
}
