package cn.cnic.bigdatalab.compute.notification

import java.util.Properties
import java.util.concurrent._
import cn.cnic.bigdatalab.utils.PropertyUtil

import kafka.consumer.Consumer
import kafka.consumer.ConsumerConfig
import kafka.utils.Logging
import kafka.consumer.KafkaStream
/**
  * Created by xjzhu@cnic.cn on 2016/8/3.
  */
class KafkaMessageConsumer(val zookeeper: String,
                           val groupId: String,
                           val topic: String,
                           val delay: Long) extends Logging{

  val config = createConsumerConfig(zookeeper, groupId)
  val consumer = Consumer.create(config)
  var executor: ExecutorService = null

  def shutdown() = {
    if (consumer != null)
      consumer.shutdown();
    if (executor != null)
      executor.shutdown();
  }

  def createConsumerConfig(zookeeper: String, groupId: String): ConsumerConfig = {
    val props = new Properties()
    props.put("zookeeper.connect", zookeeper);
    props.put("group.id", groupId);
    props.put("auto.offset.reset", "largest");
    props.put("zookeeper.session.timeout.ms", "400");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");
    val config = new ConsumerConfig(props)
    config
  }

  def runWithThreadPool(numThreads: Int) = {
    val topicCountMap = Map(topic -> numThreads)
    val consumerMap = consumer.createMessageStreams(topicCountMap);
    val streams = consumerMap.get(topic).get;

    //TODO: implement by thread
    executor = Executors.newFixedThreadPool(numThreads);
    var threadNumber = 0;
    for (stream <- streams) {
      //executor.submit(new ConsumerRunnable(stream, threadNumber, delay))
      executor.execute(new ConsumerRunnable(stream, threadNumber, delay))
      /*val it = stream.iterator()

      while (it.hasNext()) {
        val msg = new String(it.next().message());
        System.out.println(System.currentTimeMillis() + ",Thread " + threadNumber + ": " + msg);
      }*/
      threadNumber += 1
    }
  }

  def run() = {
    val topicCountMap = Map(topic -> 1)
    val consumerMap = consumer.createMessageStreams(topicCountMap);
    val streams = consumerMap.get(topic).get;

    for (stream <- streams) {

      val it = stream.iterator()

      while (it.hasNext()) {
        val msg = new String(it.next().message());
        System.out.println(System.currentTimeMillis() + ": " + msg);
      }
    }
  }
}

class ConsumerRunnable(val stream: KafkaStream[Array[Byte], Array[Byte]], val threadNumber: Int, val delay: Long) extends Logging with Runnable {
  def run {
    val it = stream.iterator()

    while (it.hasNext()) {
      val msg = new String(it.next().message());
      System.out.println(System.currentTimeMillis() + ",Thread " + threadNumber + ": " + msg);
    }

    System.out.println("Shutting down Thread: " + threadNumber);
  }
}

object KafkaMessageConsumer {
  def main(agrs: Array[String]): Unit ={


    val zookeeper = PropertyUtil.getPropertyValue("zookeeper.connect")
    val brokerList = PropertyUtil.getPropertyValue("kafka.brokerList")
    val groupId = "group1"
    val topic = "test"
    val delay = 0
    val threadNum = 3

    KafkaMessagerProducer.produce(topic,"1",brokerList,"Failed")
    KafkaMessagerProducer.produce(topic,"1",brokerList,"Successfull")
    KafkaMessagerProducer.produce(topic,"1",brokerList,"Failed")
    KafkaMessagerProducer.produce(topic,"1",brokerList,"Failed")
    val example = new KafkaMessageConsumer(zookeeper, groupId, topic, delay)
    //example.runWithThreadPool(threadNum)
    example.run()
    Thread.sleep(10000)
    example.shutdown()

  }

}
