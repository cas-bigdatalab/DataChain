package com.github.casbigdatalab.datachain.dataparser

/**
 * Created by cnic-liliang on 2016/6/3.
 */

import java.util
import org.apache.commons.csv.{CSVRecord, CSVFormat}
import java.io.{InputStreamReader, ByteArrayInputStream}

class csvdataparser(mapping_conf : String) extends common{
  val jmapping = tools.jsonfile2JsonMap(mapping_conf)
  val delimiter = tools.jsonMap2Delimiter(jmapping)
  val schemaList =tools.jsonMap2SchemaList(jmapping)

//  for(k <- 0 to schemaList.length -1) {
//    val field = schemaList(k)
//    println(field)
//  }

  def parse( msg:String): util.ArrayList[Map[String, Any]] = {
    // parser here
    val msg_in = new ByteArrayInputStream(msg.getBytes())
    val msg_reader = new InputStreamReader(msg_in)

    val records = CSVFormat.DEFAULT.withDelimiter(delimiter).parse(msg_reader)

    var k = 0
    val subrecords = records.getRecords
    //println("size of subrecords " + subrecords.size())
    //parser the whole line of csv

    val result = new util.ArrayList[Map[String, Any]]()

    //
    while(k < subrecords.size()) {
      val record = subrecords.get(k)
      //println("record_size " + record.size())
      //get each item of csv line
      if(record.size() != schemaList.length) println("field number of record(" + record.size() + ") is not equal to (" + schemaList.length + ") of dimensions" )
      var cnt = 0
      while (cnt < record.size() && cnt < schemaList.length) {
//        print(schemaList(k).get("field_name").get + ": " + record.get(cnt) + ", ")
        val map= Map(schemaList(cnt).toString -> record.get(cnt))
        result.add(map)
        cnt += 1
      }
      print("\n")

      k+=1
    }

    result
  }
}