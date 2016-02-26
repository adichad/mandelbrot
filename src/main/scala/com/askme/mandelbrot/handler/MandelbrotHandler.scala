package com.askme.mandelbrot.handler

import java.io.IOException

import akka.actor.SupervisorStrategy.{Restart, Resume}
import akka.actor.{Actor, OneForOneStrategy, Props}
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.analyse.AnalyseRouter
import com.askme.mandelbrot.handler.get.GetRouter
import com.askme.mandelbrot.handler.helper.CORS
import com.askme.mandelbrot.handler.index.IndexRouter
import com.askme.mandelbrot.handler.aggregate.AggregateRouter
import com.askme.mandelbrot.handler.search.bazaar.ProductSearchRouter
import com.askme.mandelbrot.handler.search.geo.GeoSearchRouter
import com.askme.mandelbrot.handler.search.{SearchDocsRouter, SearchRouter, DealSearchRouter}
import com.askme.mandelbrot.handler.suggest.SuggestRouter
import com.askme.mandelbrot.handler.watch.WatchRouter
import com.askme.mandelbrot.loader.FileSystemWatcher
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import spray.routing.Directive.pimpApply
import spray.routing.HttpService
import scala.language.postfixOps
import scala.concurrent.duration._



class MandelbrotHandler(val config: Config, val serverContext: SearchContext)
  extends HttpService with Actor with Logging with Configurable with CORS {

  private val aggRouter = AggregateRouter(conf("aggregate"))
  private val indexRouter = IndexRouter(conf("indexing"))

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _: IOException ⇒ Resume
      case _: NullPointerException ⇒ Resume
      case _: Exception ⇒ Restart
    }

  val fsActor = context.actorOf(Props(classOf[FileSystemWatcher], config, serverContext))

  private implicit val service: MandelbrotHandler = this
  private val route = {
    cors {
      compressResponseIfRequested() {
        decompressRequest() {
          get {
            GeoSearchRouter(this) ~ ProductSearchRouter(this) ~ DealSearchRouter(this) ~ SearchDocsRouter(this) ~
              SearchRouter(this) ~ aggRouter(this) ~ AnalyseRouter(this) ~ SuggestRouter(this) ~
              GetRouter(this)
          } ~
            post {
              GeoSearchRouter(this) ~ ProductSearchRouter(this) ~ WatchRouter(this) ~ indexRouter(this)
            }
        }
      }
    }
  }

  override final def receive: Receive = {
    runRoute(route)
  }

  override def postStop = {
    info("Kill Message received")
  }

  implicit def actorRefFactory = context
}

