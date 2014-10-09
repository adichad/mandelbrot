package com.askme.mandelbrot.server

import com.askme.mandelbrot.handler.StreamAdminHandler
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.node.NodeBuilder

import scala.concurrent.duration.DurationInt

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import spray.can.Http

class RootServer(val config: Config) extends Server with Logging {
  private implicit lazy val system = ActorSystem(string("actorSystem.name"), conf("actorSystem"))
  private val topActor = system.actorOf(Props(classOf[StreamAdminHandler], conf("handler")), string("handler.name"))
  private implicit val timeout = Timeout(int("timeout").seconds)
  private lazy val transport = IO(Http)

  override def bind {
    transport ? Http.Bind(topActor, interface = string("host"), port = int("port"))
    info("server bound: " + string("host") + ":" + int("port"))
  }

  override def close {
    transport ? Http.Unbind
    system.stop(topActor)
    system.shutdown
    Thread.sleep(200)
    info("server shutdown complete: " + string("host") + ":" + int("port"))
  }

}

