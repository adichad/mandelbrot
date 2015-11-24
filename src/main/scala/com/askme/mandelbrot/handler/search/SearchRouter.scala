package com.askme.mandelbrot.handler.search

import akka.actor.Props
import com.askme.mandelbrot.handler._
import com.askme.mandelbrot.handler.message.IndexParams
import com.askme.mandelbrot.handler.search.message._
import spray.http.MediaTypes._
import spray.http.{HttpRequest, RemoteAddress}
import com.askme.mandelbrot.util.Utils._

/**
 * Created by adichad on 31/03/15.
 */
case object SearchRouter extends Router {

  override def apply(implicit service: MandelbrotHandler) = {
    import service._
    clientIP { (clip: RemoteAddress) =>
      requestInstance { (httpReq: HttpRequest) =>
        jsonpWithParameter("callback") {
          path("search" / Segment / Segment) { (index, esType) =>
            parameters('kw.as[String] ? "", 'city ? "", 'area ? "", 'pin ? "",
              'category ? "", 'id ? "", 'userid.as[Int] ? 0, 'locid.as[String]? "",
              'size.as[Int] ? 20, 'offset.as[Int] ? 0,
              'lat.as[Double] ? 0.0d, 'lon.as[Double] ? 0.0d, 'fromkm.as[Double] ? 0d, 'tokm.as[Double] ? 20.0d,
              'explain.as[Boolean] ? false, 'select ? "_id",
              'agg.as[Boolean] ? true,
              'collapse.as[Boolean] ? false, 'goldcollapse.as[Boolean] ? false, 'randomize.as[Boolean] ? true,
              'version.as[Int] ? 2,
              'client_ip.as[String] ? "") { (kw, city, area, pin,
               category, id, userid, locid,
               size, offset,
               lat, lon, fromkm, tokm,
               explain, select,
               agg, collapse, goldcollapse, randomize, version,
               trueClient) =>
              val fuzzyprefix = 2
              val fuzzysim = 1f
              val slugFlag = true
              val maxdocspershard = 50000
              val sort = "_distance,_score"
              val unselect = "keywords"
              val searchType = "query_then_fetch"
              val timeoutms = 600l
              val aggbuckets = 10
              val source = true

              respondWithMediaType(`application/json`) { ctx =>
                  context.actorOf(Props(classOf[SearchRequestCompleter], config, serverContext, ctx, SearchParams(
                    RequestParams(httpReq, clip, trueClient),
                    IndexParams(index, esType),
                    TextParams(kw.nonEmptyOrElse(category), fuzzyprefix, fuzzysim),
                    GeoParams(city, area, pin, lat, lon, fromkm, tokm),
                    FilterParams(category, id, userid, locid), PageParams(size, offset),
                    ViewParams(source, agg, aggbuckets, explain, select, unselect, searchType, slugFlag, collapse, goldcollapse, randomize, version),
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
