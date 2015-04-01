package com.askme.mandelbrot.handler.search

import akka.actor.Props
import com.askme.mandelbrot.handler._
import com.askme.mandelbrot.handler.message.IndexParams
import com.askme.mandelbrot.handler.search.message._
import spray.http.MediaTypes._
import spray.http.{HttpRequest, RemoteAddress}

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
              'category ? "", 'id ? ""
              ,
              'size.as[Int] ? 20, 'offset.as[Int] ? 0,
              'lat.as[Double] ? 0.0d, 'lon
                .as[Double] ? 0.0d, 'fromkm.as[Double] ? 0d, 'tokm.as[
                Double] ? 20.0d,
              'source.as[Boolean] ? false, 'explain.as
                [Boolean] ? false,
              'sort ? "_distance,_score",
              'select ?
                "_id",
              'agg.as[Boolean]
                ? true,
              'aggbuckets.as[Int] ? 10,
              'maxdocspershard.as[Int] ? 1000,
              'timeoutms.as[Long] ?
                3000l,
              'searchtype.as[String] ?
                "query_then_fetch",
              'client_ip.as[String] ? "") {
              (kw, city, area, pin, category, id,
               size, offset,
               lat, lon, fromkm, tokm,
               source, explain,
               sort, select,
               agg, aggbuckets,
               maxdocspershard, timeoutms, searchType, trueClient) =>
                val fuzzyprefix = 3
                val fuzzysim = 1f
                val slugFlag = true
                respondWithMediaType(`application/json`) { ctx =>
                  context.actorOf(Props(classOf[SearchRequestCompleter], config, serverContext, ctx, SearchParams(
                    RequestParams(httpReq, clip, trueClient),
                    IndexParams(index, esType),
                    TextParams(kw, fuzzyprefix, fuzzysim),
                    GeoParams(city, area, pin, lat, lon, fromkm, tokm),
                    FilterParams(category, id), PageParams(size, offset),
                    ViewParams(source, agg, aggbuckets, explain, if (lat != 0d || lon != 0d) "_distance,_score" else "_score", select, searchType, slugFlag),
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
