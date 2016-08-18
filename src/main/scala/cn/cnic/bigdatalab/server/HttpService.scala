package cn.cnic.bigdatalab.server

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.HttpHeader.ParsingResult.Ok
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.server.Directives
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import spray.json.DefaultJsonProtocol


/**
  * Created by xjzhu@cnic.cn on 2016/8/1.
  */


case class RealTimeTaskRequest(agentId:String, taskId: String)

trait Service extends DefaultJsonProtocol with Directives with SprayJsonSupport{

  implicit val realTimeTaskRequestFormat = jsonFormat2(RealTimeTaskRequest)

  val routes = {
    logRequestResult("datachain-http-service") {
      pathPrefix("task" / "v1" / "create") {
        (get & path(Segment)) { id =>
          complete {
            /*add your own code*/
            println(id)
            HttpResponse(entity = "Get 200 OK!")
          }
        } ~
        (post & entity(as[RealTimeTaskRequest])) { rRequest:RealTimeTaskRequest =>
          complete {
            /*add your own code*/
            API.runRealTimeTask(rRequest.agentId, rRequest.taskId)
            HttpResponse(entity = "Post 200 OK!")
          }
        }
      } ~
        pathPrefix("task" / "v1" / "delete") {
          (delete & path(Segment)){ name =>
            complete {
              API.deleteTask(name)
              HttpResponse(entity = " Delete 200 OK!")
            }
          }
        }
    }
  }

}

object HttpService extends App with Service{
  implicit val system = ActorSystem("HttpService", ConfigFactory.load())
  implicit val executor = system.dispatcher
  implicit val materializer = ActorMaterializer()

  val config = ConfigFactory.load()
  val logger = Logging(system, getClass)

  //Http().bindAndHandle(routes, config.getString("http.interface"), config.getInt("http.port"))
  Http().bindAndHandle(routes, "192.168.13.172", 9000)
}
