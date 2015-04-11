package com.askme.mandelbrot.handler.aggregate

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, OneForOneStrategy, Props, ReceiveTimeout}
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.aggregate.message.AggregateParams
import com.askme.mandelbrot.handler.message.{RestMessage, Timeout}
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.json4s._
import spray.http.StatusCode
import spray.http.StatusCodes._
import spray.httpx.Json4sSupport
import spray.routing.RequestContext

import scala.concurrent.duration.{Duration, MILLISECONDS}


class AggregateRequestCompleter(val config: Config, serverContext: SearchContext, requestContext: RequestContext, aggParams: AggregateParams) extends Actor with Configurable with Json4sSupport with Logging {
  val json4sFormats = DefaultFormats
  import aggParams._
   if(req.trueClient.startsWith("42.120.")) {
     warn("[" + req.clip.toString + "]->[" + req.httpReq.uri + "] [alibaba sala]")
     complete(BadRequest, "invalid request source: " + req.trueClient)
   }
   else if(lim.timeoutms>30000) {
     warn("[" + req.clip.toString + "]->[" + req.httpReq.uri + "] [invalid timeout requested]")
     complete(BadRequest, "invalid timeout: " + lim.timeoutms)
   }
   else {
     val target = context.actorOf(Props(classOf[AggregateRequestHandler], config, serverContext))
     context.setReceiveTimeout(Duration(lim.timeoutms * 2, MILLISECONDS))
     target ! aggParams
   }

   override def receive = {
     case tout: ReceiveTimeout => {
       val timeTaken = System.currentTimeMillis - startTime
       warn("[timeout/" + (timeTaken) + "] [" + req.clip.toString + "]->[" + req.httpReq.uri + "]")
       complete(GatewayTimeout, Timeout(lim.timeoutms*2))
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
         val timeTaken = System.currentTimeMillis - startTime
         error("[" + timeTaken + "] [" + req.clip.toString + "]->[" + req.httpReq.uri + "]", e)
         complete(InternalServerError, e.getMessage)
         Stop
       }
     }
 }
