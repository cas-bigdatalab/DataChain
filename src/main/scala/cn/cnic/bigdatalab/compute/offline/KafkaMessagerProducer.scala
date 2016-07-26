package cn.cnic.bigdatalab.compute.offline

import java.text.SimpleDateFormat
import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}

/**
  * Created by xjzhu@cnic.cn on 2016/7/25.
  */
object KafkaMessagerProducer {

  def produce(topic : String, partition:String , brokers : String, status: String = "Finished"): Unit ={


    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    //props.put("partitioner.class", "com.colobu.kafka.SimplePartitioner")
    props.put("producer.type", "async")
    //props.put("request.required.cks", "1")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)
    val msg = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) + ":" + status +"!"
    val data = new KeyedMessage[String, String](topic, partition, msg)
    producer.send(data)

    producer.close
  }

  def main(agrs: Array[String]): Unit ={
    KafkaMessagerProducer.produce("notification","1","10.0.71.20:9092,10.0.71.26:9092,10.0.71.27:9092")
  }

}
