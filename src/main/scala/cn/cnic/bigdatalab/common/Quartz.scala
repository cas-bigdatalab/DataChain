package cn.cnic.bigdatalab.common

import akka.actor.ActorSystem
import com.typesafe.akka.extension.quartz.QuartzSchedulerExtension

/**
  * Created by Flora on 2016/7/27.
  */
object Quartz {
  val system = ActorSystem("offline-timer")
  var tasks = Map[String, Any]()
  val qse = QuartzSchedulerExtension(system)

}
