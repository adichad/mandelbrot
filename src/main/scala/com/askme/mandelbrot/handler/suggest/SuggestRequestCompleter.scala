package com.askme.mandelbrot.handler.suggest

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, OneForOneStrategy, Props, ReceiveTimeout}
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.EmptyResponse
import com.askme.mandelbrot.handler.message.{ErrorResponse, RestMessage, Timeout}
import com.askme.mandelbrot.handler.suggest.message.SuggestParams
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.json4s._
import spray.http.StatusCode
import spray.http.StatusCodes._
import spray.httpx.Json4sSupport
import spray.routing.RequestContext

import scala.concurrent.duration.{Duration, MILLISECONDS}

class SuggestRequestCompleter(val config: Config, serverContext: SearchContext, requestContext: RequestContext, suggestParams: SuggestParams) extends Actor with Configurable with Json4sSupport with Logging {
  val json4sFormats = DefaultFormats

  private val blockedIPs = list[String]("search.block.ip")
  private val blockedPrefixes = list[String]("search.block.ip-prefix")

  if(blockedPrefixes.exists(suggestParams.req.trueClient.startsWith(_))
    || blockedIPs.exists(suggestParams.req.trueClient == _)) {
    warn("[" + suggestParams.req.clip.toString + "]->[" + suggestParams.req.httpReq.uri + "] [invalid request source]")
    complete(BadRequest, "invalid request source")
  }
  else if(suggestParams.page.offset < int("search.offset.min") || suggestParams.page.offset > int("search.offset.max")) {
    warn("[" + suggestParams.req.clip.toString + "]->[" + suggestParams.req.httpReq.uri + "] [invalid offset]")
    complete(BadRequest, "invalid offset: " + suggestParams.page.offset)
  }
  else if(suggestParams.page.size < int("search.size.min") || suggestParams.page.size > int("search.size.max")) {
    warn("[" + suggestParams.req.clip.toString + "]->[" + suggestParams.req.httpReq.uri + "] [invalid page size]")
    complete(BadRequest, "invalid page size: " + suggestParams.page.size)
  }
  else if(suggestParams.limits.timeoutms>int("search.timeoutms.max")) {
    warn("[" + suggestParams.req.clip.toString + "]->[" + suggestParams.req.httpReq.uri + "] [invalid timeout requested]")
    complete(BadRequest, "invalid timeout: " + suggestParams.limits.timeoutms)
  }
  else if(suggestParams.target.kw.length > 18) {
    warn("[" + suggestParams.req.clip.toString + "]->[" + suggestParams.req.httpReq.uri + "] [invalid kw length]")
    complete(BadRequest, "invalid kw parameter size: " + suggestParams.target.kw.length)
  }
  else if(suggestParams.target.kw.toLowerCase.contains("utm_")) {
    warn("[" + suggestParams.req.clip.toString + "]->[" + suggestParams.req.httpReq.uri + "] [invalid kw: utm_]")
    complete(BadRequest, "invalid kw parameter: " + suggestParams.target.kw)
  }
  else if(suggestParams.geo.area.length > int("search.area-length.max")) {
    warn("[" + suggestParams.req.clip.toString + "]->[" + suggestParams.req.httpReq.uri + "] [invalid area length]")
    complete(BadRequest, "invalid area parameter size: " + suggestParams.target.kw.length)
  }
  else {

    val target =
        context.actorOf (Props (classOf[SuggestRequestHandler], config, serverContext))

    context.setReceiveTimeout(Duration(suggestParams.limits.timeoutms * 10, MILLISECONDS))
    target ! suggestParams
  }

  override def receive = {
    case _: EmptyResponse => {
      val timeTaken = System.currentTimeMillis - suggestParams.startTime
      warn("[" + timeTaken + "] [" + suggestParams.req.clip.toString + "]->[" + suggestParams.req.httpReq.uri + "] [empty search criteria]")
      complete(BadRequest, EmptyResponse("empty search criteria"))
    }
    case tout: ReceiveTimeout => {
      val timeTaken = System.currentTimeMillis - suggestParams.startTime
      warn("[timeout/" + (timeTaken) + "] [" + suggestParams.req.clip.toString + "]->[" + suggestParams.req.httpReq.uri + "]")
      complete(GatewayTimeout, Timeout(timeTaken, suggestParams.limits.timeoutms*10))
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
        val timeTaken = System.currentTimeMillis - suggestParams.startTime
        error("[" + timeTaken + "] [" + suggestParams.req.clip.toString + "]->[" + suggestParams.req.httpReq.uri + "]", e)
        complete(InternalServerError, e)
        Stop
      }
    }
}
