package com.askme.mandelbrot.handler.get

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, OneForOneStrategy, Props, ReceiveTimeout}
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.EmptyResponse
import com.askme.mandelbrot.handler.get.message.GetParams
import com.askme.mandelbrot.handler.message.{ErrorResponse, RestMessage, Timeout}
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.json4s._
import spray.http.StatusCode
import spray.http.StatusCodes._
import spray.httpx.Json4sSupport
import spray.routing.RequestContext

import scala.concurrent.duration.{Duration, MILLISECONDS}

class GetRequestCompleter(val parentPath: String, serverContext: SearchContext, requestContext: RequestContext, getParams: GetParams) extends Actor with Configurable with Json4sSupport with Logging {
  val json4sFormats = DefaultFormats

  val target = context.actorOf (Props (classOf[GetRequestHandler], parentPath, serverContext))
  context.setReceiveTimeout(Duration(500, MILLISECONDS))
  target ! getParams

  override def receive = {
    case empty: EmptyResponse => {
      complete(NotFound, empty)
    }
    case tout: ReceiveTimeout => {
      val timeTaken = System.currentTimeMillis - getParams.startTime
      warn("[timeout/" + timeTaken + "] [" + getParams.clip.toString + "]->[" + getParams.req.uri + "]")
      complete(GatewayTimeout, Timeout(timeTaken, 500))
    }
    case err: ErrorResponse => complete(InternalServerError, err.message)
    case res: RestMessage => complete(OK, res)
  }

  def complete[T <: AnyRef](status: StatusCode, obj: T) = {
    requestContext.complete(status, obj)
    context.stop(self)
  }


  override val supervisorStrategy =
    OneForOneStrategy() {
      case e => {
        val timeTaken = System.currentTimeMillis - getParams.startTime
        error("[" + timeTaken + "] [" + getParams.clip.toString + "]->[" + getParams.req.uri + "]", e)
        complete(InternalServerError, e)
        Stop
      }
    }
}
