package com.askme.mandelbrot.handler.index

import akka.actor.Props
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler._
import com.askme.mandelbrot.handler.message.IndexParams
import com.typesafe.config.Config
import spray.http.MediaTypes._
import spray.http.{HttpRequest, RemoteAddress, StatusCodes}

/**
 * Created by adichad on 31/03/15.
 */
case class IndexRouter(val config: Config) extends Router with Configurable {

  override def apply(implicit service: MandelbrotHandler) = {
    import service._
    clientIP { (clip: RemoteAddress) =>
      requestInstance { (httpReq: HttpRequest) =>
        path("index" / Segment / Segment ) { (index, esType) =>
          if(boolean("enabled")) {
            entity(as[Array[Byte]]) { data =>
              respondWithMediaType(`application/json`) {
                ctx => context.actorOf(Props(classOf[IndexRequestCompleter], service.config, serverContext, ctx,
                  IndexingParams(
                    RequestParams(httpReq, clip, clip.toString),
                    IndexParams(index, esType),
                    RawData(new String(data, "Windows-1252")),
                    System.currentTimeMillis
                  )))
              }
            }
          } else {
            respondWithMediaType(`application/json`) {
              complete(StatusCodes.MethodNotAllowed, "unsupported operation")
            }
          }
        }
      }
    }
  }

}
