package com.askme.mandelbrot.handler.analyse

import java.nio.charset.Charset

import akka.actor.Props
import com.askme.mandelbrot.handler.MandelbrotHandler
import com.askme.mandelbrot.handler.RequestParams
import com.askme.mandelbrot.handler.Router
import com.askme.mandelbrot.handler._
import com.askme.mandelbrot.handler.analyse.message.AnalyseParams
import com.askme.mandelbrot.handler.message.IndexParams
import com.askme.mandelbrot.handler.message.IndexParams
import com.askme.mandelbrot.handler.search.message._
import spray.http.MediaTypes._
import spray.http.{HttpRequest, RemoteAddress}
import com.askme.mandelbrot.util.Utils._

/**
 * Created by adichad on 05/06/15.
 */
case object AnalyseRouter extends Router {

  override def apply(implicit service: MandelbrotHandler) = {
    import service._
    clientIP { (clip: RemoteAddress) =>
      requestInstance { (httpReq: HttpRequest) =>
        jsonpWithParameter("callback") {
          path("analyze" / Segment / Segment) { (index, esType) =>
            parameterMultiMap { params =>
              val keywords = params.getOrElse("kw", List())
              val analysers = params.getOrElse("an", List("keyword_analyzer"))
              val trueClient = params.getOrElse("client_ip", List(""))(0)
              val charset = params.getOrElse("charset", List("utf-8"))(0)
              extract(_.request.entity.data.asString(Charset.forName(charset))) { data =>
                respondWithMediaType(`application/json`) { ctx =>
                  context.actorOf(Props(classOf[AnalyseRequestCompleter], parentPath, serverContext, ctx,
                    AnalyseParams(
                      RequestParams(httpReq, clip, trueClient),
                      IndexParams(index, esType),
                      keywords, data, analysers,
                      System.currentTimeMillis
                    ))
                  )
                }
              }
            }
          }
        }
      }
    }
  }

}
