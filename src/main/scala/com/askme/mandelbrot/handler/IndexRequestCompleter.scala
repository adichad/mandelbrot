package com.askme.mandelbrot.handler

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, OneForOneStrategy, Props}
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.json4s._
import spray.http.StatusCode
import spray.http.StatusCodes._
import spray.httpx.Json4sSupport
import spray.routing.RequestContext


/**
 * Created by adichad on 08/01/15.
 */


class IndexRequestCompleter(val config: Config, serverContext: SearchContext, requestContext: RequestContext, indexParams: IndexingParams) extends Actor with Configurable with Json4sSupport with Logging {
  val json4sFormats = DefaultFormats
  if(indexParams.req.trueClient.startsWith("42.120.")) {
    warn("[" + indexParams.req.clip.toString + "]->[" + indexParams.req.httpReq.uri + "] [alibaba sala]")
    complete(BadRequest, "invalid request source: " + indexParams.req.trueClient)
  }
  else {
    val target = context.actorOf(Props(classOf[IndexRequestHandler], config, serverContext))
    target ! indexParams
  }

  override def receive = {
    case res: RestMessage => complete(OK, res)

  }

  def complete[T <: AnyRef](status: StatusCode, obj: T) = {
    requestContext.complete(status, obj)
    context.stop(self)
  }


  override val supervisorStrategy =
    OneForOneStrategy() {
      case e => {
        val timeTaken = System.currentTimeMillis - indexParams.startTime
        error("[" + timeTaken + "] [" + indexParams.req.clip.toString + "]->[" + indexParams.req.httpReq.uri + "]", e)
        complete(InternalServerError, e.getMessage)
        Stop
      }
    }
}
