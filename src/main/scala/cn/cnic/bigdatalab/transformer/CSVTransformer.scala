 package cn.cnic.bigdatalab.transformer

/**
 * Created by cnic-liliang on 2016/6/3.
 */

import java.util
 import cn.cnic.bigdatalab.utils.FieldTypeUtil
 import org.apache.commons.csv.{CSVRecord, CSVFormat}
import java.io.{InputStreamReader, ByteArrayInputStream}

 import scala.collection.mutable.ArrayBuffer

 class CSVTransformer(tmap : TransformerMapping) extends TransformerBase{
  val delimiter:Char = tmap.delimiter
  val schema:ArrayBuffer[String] =tmap.dimensions

   def getSchema():ArrayBuffer[String] = {
     schema
   }

   def multiLineTransform(msg: String):ArrayBuffer[ArrayBuffer[Any]] = {
     return new ArrayBuffer[ArrayBuffer[Any]]()
   }

  def transform( msg:String): ArrayBuffer[Any] = {
    // parser here
    val msg_in = new ByteArrayInputStream(msg.getBytes())
    val msg_reader = new InputStreamReader(msg_in)
    //csv parser
    val records = CSVFormat.DEFAULT.withDelimiter(delimiter).parse(msg_reader)
    var k = 0
    val subrecords = records.getRecords
    println("size of subrecords " + subrecords.size())
    //parser the whole line of csv
    //columns
    val columns = tmap.columns
    //val columnsTypeList = tools.jsonMap2ColumnTypes(jmapping)

    //val result = new util.ArrayList[String]()

    var row = new ArrayBuffer[Any]()
    //traverse
    while(k < subrecords.size()) {
      val record = subrecords.get(k)
      //println("record_size " + record.size())
      //get each item of csv line
      if(record.size() != columns.length) println("field number of record(" + record.size() + ") is not equal to (" + columns.length + ") of dimensions" )
      var cnt = 0
      while (cnt < record.size() && cnt < columns.length) {
//        println(columnslist(cnt) + ": " + record.get(cnt) + ", ")
        //extract
        if(schema.toList.contains(columns(cnt))) {
          //val value = FieldTypeUtil.parseDataType(columns(cnt).toString.split(":")(1), record.get(cnt))
          val value = tools.valueDataType(columns(cnt).toString.split(":")(1), record.get(cnt).trim, tmap)
          row += value
        }
        cnt += 1
      }
      k+=1
    }
    row
  }
}