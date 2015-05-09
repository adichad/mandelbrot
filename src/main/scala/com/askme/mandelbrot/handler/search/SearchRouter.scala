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
            parameters(
              'kw.as[String] ? "", 'city
                ? "", 'area ? "", 'pin ? "",
              'category ? "", 'id ? "",
              'userid.as[Int] ? 0, 'locid.as[String]? "",
              'size.as[Int] ? 20, 'offset.as[Int] ? 0,
              'lat.as[Double] ? 0.0d, 'lon
                .as[Double] ? 0.0d, 'fromkm.as[Double] ? 0d, 'tokm.as[
                Double] ? 20.0d,
              'source.as[Boolean] ? false, 'explain.as
                [Boolean] ? false,
              'select ?
                "_id",
              'agg.as[Boolean]
                ? true,
              'aggbuckets.as[Int] ? 10,

              'timeoutms.as[Long] ?
                1500l,
              'searchtype.as[String] ?
                "query_then_fetch",
              'client_ip.as[String] ? "") {
              (kw, city, area, pin, category, id, userid, locid,
               size, offset,
               lat, lon, fromkm, tokm,
               source, explain,
               select,
               agg, aggbuckets,
               timeoutms, searchType, trueClient) =>
                val fuzzyprefix = 3
                val fuzzysim = 1f
                val slugFlag = true
                val maxdocspershard = 50000
                val sort = "_distance,_score"
                val unselect = "keywords"
              respondWithMediaType(`application/json`) { ctx =>
                  context.actorOf(Props(classOf[SearchRequestCompleter], config, serverContext, ctx, SearchParams(
                    RequestParams(httpReq, clip, trueClient),
                    IndexParams(index, esType),
                    TextParams(kw.nonEmptyOrElse(category), fuzzyprefix, fuzzysim),
                    GeoParams(city, area, pin, lat, lon, fromkm, tokm),
                    FilterParams(category, id, userid, locid), PageParams(size, offset),
                    ViewParams(source, agg, aggbuckets, explain, if (lat != 0d || lon != 0d) "_distance,_score" else "_score", select, unselect, searchType, slugFlag),
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
