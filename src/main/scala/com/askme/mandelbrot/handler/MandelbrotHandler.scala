package com.askme.mandelbrot.handler

import java.io.IOException

import akka.actor.SupervisorStrategy.{Restart, Resume}
import akka.actor.{Actor, OneForOneStrategy, Props}
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.helper.CORS
import com.askme.mandelbrot.handler.index.IndexRouter
import com.askme.mandelbrot.handler.list.AggregateRouter
import com.askme.mandelbrot.handler.search.{SearchDocsRouter, SearchRouter}
import com.askme.mandelbrot.handler.watch.WatchRouter
import com.askme.mandelbrot.loader.FileSystemWatcher
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import spray.routing.Directive.pimpApply
import spray.routing.HttpService

import scala.concurrent.duration._



class MandelbrotHandler(val config: Config, val serverContext: SearchContext)
  extends HttpService with Actor with Logging with Configurable with CORS {

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _: IOException ⇒ Resume
      case _: NullPointerException ⇒ Resume
      case _: Exception ⇒ Restart
    }

  val fsActor = context.actorOf(Props(classOf[FileSystemWatcher], config, serverContext))

  private implicit val service: MandelbrotHandler = this
  private val route =
    cors {
      get { SearchDocsRouter(this) ~ SearchRouter(this) ~ AggregateRouter(conf("http.aggregate"))(this) } ~
      post { WatchRouter.apply ~ IndexRouter(conf("http.indexing")).apply }
    }

  override final def receive: Receive = {
    runRoute(route)
  }

  override def postStop = {
    info("Kill Message received")
  }

  implicit def actorRefFactory = context
}

