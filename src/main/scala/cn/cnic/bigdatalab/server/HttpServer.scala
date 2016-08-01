package cn.cnic.bigdatalab.server

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.{HttpResponse, HttpRequest}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink

import scalaz.concurrent.Future

/**
  * Created by xjzhu@cnic.cn on 2016/8/1.
  */
object HttpServer extends App{

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  val serverSource = Http().bind(interface = "localhost", port = 9000)

  val requestHandler: HttpRequest => HttpResponse = {
    case HttpRequest(GET, Uri.Path("/"), _, _, _) =>
      HttpResponse(entity = "200 OK!")
    case HttpRequest(POST, Uri.Path("/test"), _, _, _)=>
      HttpResponse(entity = "200 OK!")
    case _: HttpRequest =>
      HttpResponse(404, entity = "Unknown resource!")
  }


  serverSource.to(Sink.foreach { connection =>
    println("Accepted new connection from " + connection.remoteAddress)
    connection handleWithSyncHandler requestHandler
  }).run()

}
