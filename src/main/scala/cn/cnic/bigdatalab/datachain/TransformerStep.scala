package cn.cnic.bigdatalab.datachain

import cn.cnic.bigdatalab.transformer.{TransformerMapping, Transformer}

/**
  * Created by cnic on 2016/6/21.
  */
class TransformerStep extends Step{

  var transformer:Transformer = _

  def getTransformer():Transformer ={
    assert(transformer!=null)
    transformer
  }

  def setTransformer(mapping:TransformerMapping):TransformerStep ={

    if (transformer == null){
      assert(mapping!=null)
      transformer = new Transformer(mapping.toString)
    }

    this
  }

  override def run: Unit = ???
}
