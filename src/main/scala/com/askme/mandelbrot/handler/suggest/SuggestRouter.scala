package com.askme.mandelbrot.handler.suggest

import akka.actor.Props
import com.askme.mandelbrot.handler._
import com.askme.mandelbrot.handler.message.IndexParams
import com.askme.mandelbrot.handler.search.message._
import com.askme.mandelbrot.handler.suggest.message.{SuggestParams, SuggestViewParams, TargetingParams}
import com.askme.mandelbrot.util.Utils._
import spray.http.MediaTypes._
import spray.http.{HttpRequest, RemoteAddress}

/**
 * Created by adichad on 31/03/15.
 */
case object SuggestRouter extends Router {

  override def apply(implicit service: MandelbrotHandler) = {
    import service._
    clientIP { (clip: RemoteAddress) =>
      requestInstance { (httpReq: HttpRequest) =>
        jsonpWithParameter("callback") {
          path("suggest" / Segment) { (index) =>
            parameters('kw.as[String] ? "", 'city ? "", 'area ? "", 'pin ? "",
              'tag ? "", 'id ? "",
              'size.as[Int] ? 20, 'offset.as[Int] ? 0,
              'lat.as[Double] ? 0.0d, 'lon.as[Double] ? 0.0d, 'fromkm.as[Double] ? 0d, 'tokm.as[Double] ? 20.0d,
              'explain.as[Boolean] ? false, 'select ? "*",
              'version.as[Int] ? 1,
              'client_ip.as[String] ? "") { (kw, city, area, pin,
               tag, id,
               size, offset,
               lat, lon, fromkm, tokm,
               explain, select, version,
               trueClient) =>
              val maxdocspershard = 5000
              val sort = "_distance,_score"
              val unselect = ""
              val searchType = "dfs_query_then_fetch"
              val timeoutms = 200l


              respondWithMediaType(`application/json`) { ctx =>
                  context.actorOf(Props(classOf[SuggestRequestCompleter], parentPath, serverContext, ctx, SuggestParams(
                    RequestParams(httpReq, clip, trueClient),
                    IndexParams(index, "suggestion"),
                    TargetingParams(kw, tag, id),
                    GeoParams(city, area, pin, lat, lon),
                    PageParams(size, offset),
                    SuggestViewParams(explain, select, unselect, searchType, version),
                    LimitParams(maxdocspershard, timeoutms),
                    System.currentTimeMillis
                  )))
                }
            }
          }
        }
      }
    }
  }

}
