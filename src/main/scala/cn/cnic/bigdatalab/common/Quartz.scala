package cn.cnic.bigdatalab.common

import akka.actor.ActorSystem
import com.typesafe.akka.extension.quartz.QuartzSchedulerExtension
import com.typesafe.config.ConfigFactory

import scala.collection.mutable

/**
  * Created by Flora on 2016/7/27.
  */
object Quartz {
  val system = ActorSystem("offline-timer", ConfigFactory.load())
  var tasks = mutable.Map[String, Any]()
  val qse = QuartzSchedulerExtension(system)

}
