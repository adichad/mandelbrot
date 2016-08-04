package com.askme.mandelbrot.handler.analyse

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{OneForOneStrategy, ReceiveTimeout, Props, Actor}
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.analyse.message.AnalyseParams
import com.askme.mandelbrot.handler.message.{RestMessage, Timeout}
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.json4s.DefaultFormats
import spray.http.StatusCode
import spray.http.StatusCodes._
import spray.httpx.Json4sSupport
import spray.routing.RequestContext

import scala.concurrent.duration._

/**
 * Created by adichad on 05/06/15.
 */
class AnalyseRequestCompleter(val parentPath: String, serverContext: SearchContext, requestContext: RequestContext, analyseParams: AnalyseParams) extends Actor with Configurable with Json4sSupport with Logging {
  val json4sFormats = DefaultFormats
  if(analyseParams.req.trueClient.startsWith("42.120.")) {
    warn("[" + analyseParams.req.clip.toString + "]->[" + analyseParams.req.httpReq.uri + "] [alibaba sala]")
    complete(BadRequest, "invalid request source: " + analyseParams.req.trueClient)
  }
  else {
    val target = context.actorOf(Props(classOf[AnalyseRequestHandler], parentPath, serverContext))
    target ! analyseParams
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
        val timeTaken = System.currentTimeMillis - analyseParams.startTime
        error("[" + timeTaken + "] [" + analyseParams.req.clip.toString + "]->[" + analyseParams.req.httpReq.uri + "]", e)
        complete(InternalServerError, e.getMessage)
        Stop
      }
    }
}