package com.askme.mandelbrot.handler

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, OneForOneStrategy, Props, ReceiveTimeout}
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import org.json4s._
import spray.http.StatusCode
import spray.http.StatusCodes._
import spray.httpx.Json4sSupport
import spray.routing.RequestContext


/**
 * Created by adichad on 08/01/15.
 */


class SearchRequestCompleter(val config: Config, serverContext: SearchContext, requestContext: RequestContext, searchParams: SearchParams) extends Actor with Configurable with Json4sSupport {

  val json4sFormats = DefaultFormats
  private lazy val target = context.actorOf(Props(classOf[SearchRequestHandler], config, serverContext))


  //context.setReceiveTimeout(Math.min(searchParams.limits.timeoutms, long("timeoutms")+50l) milliseconds)
  target ! searchParams


  override def receive = {
    case res: RestMessage => complete(OK, res)
    case ReceiveTimeout   => complete(GatewayTimeout, "{ \"response\": \"Request timeout\" }")
  }

  def complete[T <: AnyRef](status: StatusCode, obj: T) = {
    requestContext.complete(status, obj)
    context.stop(self)
  }


  override val supervisorStrategy =
    OneForOneStrategy() {
      case e => {
        complete(InternalServerError, e.getMessage)
        Stop
      }
    }
}
