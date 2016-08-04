package com.askme.mandelbrot.handler.get

import akka.actor.Props
import com.askme.mandelbrot.handler._
import com.askme.mandelbrot.handler.get.message.GetParams
import com.askme.mandelbrot.handler.message.IndexParams
import com.askme.mandelbrot.handler.search.message._
import com.askme.mandelbrot.handler.suggest.message.{SuggestParams, SuggestViewParams, TargetingParams}
import com.askme.mandelbrot.util.Utils._
import spray.http.MediaTypes._
import spray.http.{HttpRequest, RemoteAddress}

/**
 * Created by adichad on 31/03/15.
 */
case object GetRouter extends Router {

  override def apply(implicit service: MandelbrotHandler) = {
    import service._
    clientIP { (clip: RemoteAddress) =>
      requestInstance { (httpReq: HttpRequest) =>
        jsonpWithParameter("callback") {
          path("get" / Segment / Segment) { (esType, id) =>
            parameters('select ? "*", 'transform ? false) { (select, transform) =>
              respondWithMediaType(`application/json`) { ctx =>
                context.actorOf(
                  Props(
                    classOf[GetRequestCompleter], parentPath, serverContext, ctx,
                    GetParams(esType, id, select, transform, httpReq, clip, System.currentTimeMillis)
                  )
                )
              }
            }
          }
        }
      }
    }
  }

}
