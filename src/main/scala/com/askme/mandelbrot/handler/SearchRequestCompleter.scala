package com.askme.mandelbrot.handler

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, OneForOneStrategy, Props, ReceiveTimeout}
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.json4s._
import spray.http.StatusCode
import spray.http.StatusCodes._
import spray.httpx.Json4sSupport
import spray.routing.RequestContext
import scala.concurrent.duration.MILLISECONDS

import scala.concurrent.duration.Duration


/**
 * Created by adichad on 08/01/15.
 */

case class Timeout(val `timeout-ms`: Long)


class SearchRequestCompleter(val config: Config, serverContext: SearchContext, requestContext: RequestContext, searchParams: SearchParams) extends Actor with Configurable with Json4sSupport with Logging {

  val json4sFormats = DefaultFormats
  private lazy val target = context.actorOf(Props(classOf[SearchRequestHandler], config, serverContext))

  context.setReceiveTimeout(Duration(searchParams.limits.timeoutms*2, MILLISECONDS))
  target ! searchParams


  override def receive = {
    case _: EmptyResponse => {
      val timeTaken = System.currentTimeMillis - searchParams.startTime
      warn("[" + timeTaken + "] [" + searchParams.req.clip.toString + "]->[" + searchParams.req.httpReq.uri + "] [empty search criteria]")
      complete(BadRequest, "empty search criteria")
    }
    case tout: ReceiveTimeout => {
      val timeTaken = System.currentTimeMillis - searchParams.startTime
      warn("[timeout/" + (timeTaken) + "] [" + searchParams.req.clip.toString + "]->[" + searchParams.req.httpReq.uri + "]")
      complete(GatewayTimeout, Timeout(searchParams.limits.timeoutms*2))
    }
    case res: RestMessage => complete(OK, res)

  }

  def complete[T <: AnyRef](status: StatusCode, obj: T) = {
    requestContext.complete(status, obj)
    context.stop(self)
  }


  override val supervisorStrategy =
    OneForOneStrategy() {
      case e => {
        val timeTaken = System.currentTimeMillis - searchParams.startTime
        error("[" + timeTaken + "] [" + searchParams.req.clip.toString + "]->[" + searchParams.req.httpReq.uri + "]", e)
        complete(InternalServerError, e.getMessage)
        Stop
      }
    }
}
