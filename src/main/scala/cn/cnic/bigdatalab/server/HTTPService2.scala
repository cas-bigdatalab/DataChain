package cn.cnic.bigdatalab.server

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives
import spray.json.DefaultJsonProtocol
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._
import scala.util.parsing.json.JSON

/**
  * Created by Flora on 2016/8/8.
  */
object HTTPService2 extends DefaultJsonProtocol with Directives with SprayJsonSupport{

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  def toJson(entity: RequestEntity): Map[String, Any] = {
    entity match {
      case HttpEntity.Strict(_, data) =>{
        val temp = JSON.parseFull(data.utf8String)
        temp.get.asInstanceOf[Map[String, Any]]
      }
      case _ => Map()
    }
  }

  def route(req: HttpRequest): HttpResponse = req match {
    case HttpRequest(GET, Uri.Path("/"), headers, entity, protocol) => {
      HttpResponse(entity = "Get OK!")
    }

    case HttpRequest(POST, Uri.Path("/task/v2/create"), headers, entity, protocol) =>{
      val data = toJson(entity)
      if(data.get("agentId").isEmpty || data.get("taskId").isEmpty){
        HttpResponse(entity = "Param Error!")
      }else{
        API.runRealTimeTask(data.get("agentId").get.asInstanceOf[String], data.get("taskId").get.asInstanceOf[String])
        HttpResponse(entity = "Create OK!")
      }
    }

    case HttpRequest(DELETE, Uri.Path("/task/v2/delete"), headers, entity, protocol) =>{
      val data = toJson(entity)
      if(data.get("name").isEmpty){
        HttpResponse(entity = "Param Error!")
      }else{
        API.deleteTask(data.get("name").get.asInstanceOf[String])
        HttpResponse(entity = "DELETE OK!")
      }
    }
    case _: HttpRequest =>
      HttpResponse(404, entity = "Unknown resource!")
  }

  def run = {
    Http().bindAndHandleSync(route, "192.168.13.172", 9000)
  }

}

object Main {
  def main(argv: Array[String]):Unit = {
    HTTPService2.run
  }
}
