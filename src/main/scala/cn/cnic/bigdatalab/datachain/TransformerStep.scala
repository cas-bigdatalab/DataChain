package cn.cnic.bigdatalab.datachain

import com.github.casbigdatalab.datachain.transformer.Transformer

/**
  * Created by cnic on 2016/6/21.
  */
class TransformerStep {

  var transformer:Transformer = _
  var mappingFile:String = _

  def setMappingFile(mappingFile:String): TransformerStep={
    this.mappingFile = mappingFile
    this
  }

  def getTransformer(mappingFile:String):Transformer ={

    if (transformer == null){
      assert(mappingFile!=null)
      transformer = new Transformer(mappingFile)
    }

    transformer
  }


}
