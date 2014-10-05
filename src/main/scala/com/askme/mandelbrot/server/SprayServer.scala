package com.askme.mandelbrot.server

import scala.concurrent.duration.DurationInt

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import spray.can.Http

class SprayServer(val config: Config) extends Server with Logging {
  private implicit lazy val system = ActorSystem(string("system-name"))
  private val service = system.actorOf(Props(Class.forName(string("handler.type")), conf("handler")), string("handler.name"))
  private implicit val timeout = Timeout(int("timeout").seconds)
  private lazy val bindable = IO(Http)
  
  override def bind {
    bindable ? Http.Bind(service, interface = string("host"), port = int("port"))
    info("server bound: " + string("host") + ":" + int("port"))
  }

  override def close {
    bindable ? Http.Unbind
    system.shutdown
  }

}

