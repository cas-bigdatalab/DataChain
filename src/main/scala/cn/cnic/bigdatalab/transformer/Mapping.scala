package cn.cnic.bigdatalab.transformer

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Flora on 2016/6/23.
  */
class Mapping {
  var mapType:String = "csvString"
  var columns: ArrayBuffer[String] = new ArrayBuffer[String]()
  var dimensions: ArrayBuffer[String] = new ArrayBuffer[String]()
  var pattern:String = ""
  var delimiter:Char = ',' //default is ,
}
