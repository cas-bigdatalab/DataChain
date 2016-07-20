package cn.cnic.bigdatalab.compute.realtime

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by Flora on 2016/7/20.
  */
object Kafka2SparkStreaming {

  def createStream(ssc: StreamingContext, topic: String, kafkaParam: String): DStream[String] ={
    val topics = topic.replaceAll(" ", "").split(",").map { s =>
      val a = s.split(":")
      (a(0), a(1).toInt)
    }.toMap

    val kafkaParams = kafkaParam.replaceAll(" ", "").split(";").map { s =>
      val a = s.split("->")
      (a(0), a(1))
    }.toMap

    //Create Kafka Stream and currently only support kafka with String messages
    val kafkaStream = KafkaUtils.createStream[
      String,
      String,
      StringDecoder,
      StringDecoder
      ](ssc, kafkaParams, topics, StorageLevel.MEMORY_AND_DISK_SER_2)

    //Get the messages and execute operations
    kafkaStream.map(_._2)
  }
}
