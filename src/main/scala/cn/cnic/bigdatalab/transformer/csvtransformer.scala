package com.github.casbigdatalab.datachain.transformer

/**
 * Created by cnic-liliang on 2016/6/3.
 */

import java.util
import org.apache.commons.csv.{CSVRecord, CSVFormat}
import java.io.{InputStreamReader, ByteArrayInputStream}

class csvtransformer(mapping_conf : String) extends common{
  val jmapping = tools.jsonfile2JsonMap(mapping_conf)
  val delimiter = tools.jsonMap2Delimiter(jmapping)
  val schemaList =tools.jsonMap2SchemaList(jmapping)

//  for(k <- 0 to schemaList.length -1) {
//    val field = schemaList(k)
//    println(field)
//  }

  def transform( msg:String): util.ArrayList[String] = {
    // parser here
    val msg_in = new ByteArrayInputStream(msg.getBytes())
    val msg_reader = new InputStreamReader(msg_in)
    //csv parser
    val records = CSVFormat.DEFAULT.withDelimiter(delimiter).parse(msg_reader)
    var k = 0
    val subrecords = records.getRecords
    //println("size of subrecords " + subrecords.size())
    //parser the whole line of csv
    //columns
    val columnslist = tools.jsonMap2Columns(jmapping)

    val result = new util.ArrayList[String]()
    //traverse
    while(k < subrecords.size()) {
      val record = subrecords.get(k)
      //println("record_size " + record.size())
      //get each item of csv line
      if(record.size() != columnslist.length) println("field number of record(" + record.size() + ") is not equal to (" + columnslist.length + ") of dimensions" )
      var cnt = 0
      while (cnt < record.size() && cnt < columnslist.length) {
//        println(columnslist(cnt) + ": " + record.get(cnt) + ", ")
        //extract
        if(schemaList.contains(columnslist(cnt))) {
          //val map = Map(columnslist(cnt).toString -> record.get(cnt))
          result.add(columnslist(cnt).toString + ": " + record.get(cnt))
        }
        cnt += 1
      }

      k+=1
    }
    result
  }
}