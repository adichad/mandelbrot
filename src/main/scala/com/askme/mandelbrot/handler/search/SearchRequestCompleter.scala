package com.askme.mandelbrot.handler.search

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, OneForOneStrategy, Props, ReceiveTimeout}
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.message.{ErrorResponse, RestMessage, Timeout}
import com.askme.mandelbrot.handler.EmptyResponse
import com.askme.mandelbrot.handler.search.message.{DealSearchParams, SearchParams}
import com.askme.mandelbrot.handler.suggest.{SuggestRequestHandler}
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.json4s._
import spray.http.StatusCode
import spray.http.StatusCodes._
import spray.httpx.Json4sSupport
import spray.routing.RequestContext

import scala.concurrent.duration.{Duration, MILLISECONDS}

class SearchRequestCompleter(val config: Config, serverContext: SearchContext, requestContext: RequestContext, searchParams: SearchParams) extends Actor with Configurable with Json4sSupport with Logging {
  val json4sFormats = DefaultFormats

  private val blockedIPs = list[String]("search.block.ip")
  private val blockedPrefixes = list[String]("search.block.ip-prefix")

  if(blockedPrefixes.exists(searchParams.req.trueClient.startsWith(_))
    || blockedIPs.exists(searchParams.req.trueClient == _)) {
    warn("[" + searchParams.req.clip.toString + "]->[" + searchParams.req.httpReq.uri + "] [invalid request source]")
    complete(BadRequest, "invalid request source")
  }
  else if(searchParams.page.offset < int("search.offset.min") || searchParams.page.offset > int("search.offset.max")) {
    warn("[" + searchParams.req.clip.toString + "]->[" + searchParams.req.httpReq.uri + "] [invalid offset]")
    complete(BadRequest, "invalid offset: " + searchParams.page.offset)
  }
  else if(searchParams.page.size < int("search.size.min") || searchParams.page.size > int("search.size.max")) {
    warn("[" + searchParams.req.clip.toString + "]->[" + searchParams.req.httpReq.uri + "] [invalid page size]")
    complete(BadRequest, "invalid page size: " + searchParams.page.size)
  }
  else if(searchParams.limits.timeoutms>int("search.timeoutms.max")) {
    warn("[" + searchParams.req.clip.toString + "]->[" + searchParams.req.httpReq.uri + "] [invalid timeout requested]")
    complete(BadRequest, "invalid timeout: " + searchParams.limits.timeoutms)
  }
  else if(searchParams.text.kw.length > int("search.kw-length.max")) {
    warn("[" + searchParams.req.clip.toString + "]->[" + searchParams.req.httpReq.uri + "] [invalid kw length]")
    complete(BadRequest, "invalid kw parameter size: " + searchParams.text.kw.length)
  }
  else if(searchParams.geo.area.length > int("search.area-length.max")) {
    warn("[" + searchParams.req.clip.toString + "]->[" + searchParams.req.httpReq.uri + "] [invalid area length]")
    complete(BadRequest, "invalid area parameter size: " + searchParams.text.kw.length)
  }
  else {

    val target =
        searchParams.idx.esType match {
          case "list" => context.actorOf (Props (classOf[ListSearchRequestHandler], config, serverContext) )
          case "place" => context.actorOf (Props (classOf[PlaceSearchRequestHandler], config, serverContext) )
          case _ => context.actorOf (Props (classOf[PlaceSearchRequestHandler], config, serverContext) )
        }
    context.setReceiveTimeout(Duration(searchParams.limits.timeoutms * 5, MILLISECONDS))
    target ! searchParams
  }

  override def receive = {
    case _: EmptyResponse => {
      val timeTaken = System.currentTimeMillis - searchParams.startTime
      warn("[" + timeTaken + "] [" + searchParams.req.clip.toString + "]->[" + searchParams.req.httpReq.uri + "] [empty search criteria]")
      complete(BadRequest, EmptyResponse("empty search criteria"))
    }
    case tout: ReceiveTimeout => {
      val timeTaken = System.currentTimeMillis - searchParams.startTime
      warn("[timeout/" + (timeTaken) + "] [" + searchParams.req.clip.toString + "]->[" + searchParams.req.httpReq.uri + "]")
      complete(GatewayTimeout, Timeout(timeTaken, searchParams.limits.timeoutms*5))
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
        val timeTaken = System.currentTimeMillis - searchParams.startTime
        error("[" + timeTaken + "] [" + searchParams.req.clip.toString + "]->[" + searchParams.req.httpReq.uri + "]", e)
        complete(InternalServerError, e)
        Stop
      }
    }
}

class DealSearchRequestCompleter(val config: Config, serverContext: SearchContext, requestContext: RequestContext, searchParams: DealSearchParams) extends Actor with Configurable with Json4sSupport with Logging {
  val json4sFormats = DefaultFormats

  private val blockedIPs = list[String]("search.block.ip")
  private val blockedPrefixes = list[String]("search.block.ip-prefix")

  if(blockedPrefixes.exists(searchParams.req.trueClient.startsWith(_))
    || blockedIPs.exists(searchParams.req.trueClient == _)) {
    warn("[" + searchParams.req.clip.toString + "]->[" + searchParams.req.httpReq.uri + "] [invalid request source]")
    complete(BadRequest, "invalid request source")
  }
  else if(searchParams.page.offset < int("search.offset.min") || searchParams.page.offset > int("search.offset.max")) {
    warn("[" + searchParams.req.clip.toString + "]->[" + searchParams.req.httpReq.uri + "] [invalid offset]")
    complete(BadRequest, "invalid offset: " + searchParams.page.offset)
  }
  else if(searchParams.page.size < int("search.size.min") || searchParams.page.size > int("search.size.max")) {
    warn("[" + searchParams.req.clip.toString + "]->[" + searchParams.req.httpReq.uri + "] [invalid page size]")
    complete(BadRequest, "invalid page size: " + searchParams.page.size)
  }
  else if(searchParams.limits.timeoutms>int("search.timeoutms.max")) {
    warn("[" + searchParams.req.clip.toString + "]->[" + searchParams.req.httpReq.uri + "] [invalid timeout requested]")
    complete(BadRequest, "invalid timeout: " + searchParams.limits.timeoutms)
  }
  else if(searchParams.text.kw.length > int("search.kw-length.max")) {
    warn("[" + searchParams.req.clip.toString + "]->[" + searchParams.req.httpReq.uri + "] [invalid kw length]")
    complete(BadRequest, "invalid kw parameter size: " + searchParams.text.kw.length)
  }
  else if(searchParams.geo.area.length > int("search.area-length.max")) {
    warn("[" + searchParams.req.clip.toString + "]->[" + searchParams.req.httpReq.uri + "] [invalid area length]")
    complete(BadRequest, "invalid area parameter size: " + searchParams.text.kw.length)
  }
  else {

    val target =
      searchParams.idx.esType match {
        case "deal" => context.actorOf (Props (classOf[DealSearchRequestHandler], config, serverContext) )
      }
    context.setReceiveTimeout(Duration(searchParams.limits.timeoutms * 5, MILLISECONDS))
    target ! searchParams
  }

  override def receive = {
    case _: EmptyResponse => {
      val timeTaken = System.currentTimeMillis - searchParams.startTime
      warn("[" + timeTaken + "] [" + searchParams.req.clip.toString + "]->[" + searchParams.req.httpReq.uri + "] [empty search criteria]")
      complete(BadRequest, EmptyResponse("empty search criteria"))
    }
    case tout: ReceiveTimeout => {
      val timeTaken = System.currentTimeMillis - searchParams.startTime
      warn("[timeout/" + (timeTaken) + "] [" + searchParams.req.clip.toString + "]->[" + searchParams.req.httpReq.uri + "]")
      complete(GatewayTimeout, Timeout(timeTaken, searchParams.limits.timeoutms*5))
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
        val timeTaken = System.currentTimeMillis - searchParams.startTime
        error("[" + timeTaken + "] [" + searchParams.req.clip.toString + "]->[" + searchParams.req.httpReq.uri + "]", e)
        complete(InternalServerError, e)
        Stop
      }
    }
}
