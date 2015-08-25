package com.askme.mandelbrot.handler.search

import java.nio.charset.Charset

import akka.actor.Props
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler._
import com.askme.mandelbrot.handler.message.IndexParams
import com.askme.mandelbrot.handler.search.DealSearchRequestCompleter
import com.askme.mandelbrot.handler.search.message._
import com.typesafe.config.Config
import org.json4s.jackson.JsonMethods._
import spray.http.MediaTypes._
import spray.http.{HttpRequest, RemoteAddress, StatusCodes}

/**
 * Created by nishant on 30/07/15.
 */
case object DealSearchRouter extends Router {

  override def apply(implicit service: MandelbrotHandler) = {
    import service._
    clientIP { (clip: RemoteAddress) =>
      requestInstance { (httpReq: HttpRequest) =>
        path("search" / "deal") {
          parameters('what.as[String] ? "", 'city ? "", 'area ? "", 'id ? "",
            'applicableto ? "", 'agg ? true) { (kw, city, area, id, applicableTo, aggr) =>
            val size = 20
            val offset = 0
            val source = true
            val version = 1
            val fuzzyprefix = 2
            val fuzzysim = 1f
            val slugFlag = true
            val maxdocspershard = 50000
            val sort = "_distance,_score"
            val unselect = "keywords"
            val searchType = "query_then_fetch"
            val timeoutms = 600l
            val aggbuckets = 10
            respondWithMediaType(`application/json`) {
               ctx => context.actorOf(Props(classOf[DealSearchRequestCompleter], config, serverContext, ctx, DealSearchParams(
                  req = RequestParams(httpReq, clip, clip.toString()),
                  idx = IndexParams("askmedeal", "deal"),
                  text = TextParams(kw, fuzzyprefix, fuzzysim),
                  geo = GeoParams(city, area, "", 0.0d, 0.0d, 0d, 20.0d),
                  filters = DealFilterParams(id, applicableTo), page = PageParams(size, offset),
                  view = ViewParams(source, aggr, aggbuckets, false, "", unselect, searchType, slugFlag, false, version),
                  limits = LimitParams(maxdocspershard, timeoutms),
                startTime = System.currentTimeMillis
              )))
            }
          }
        }
      }
    }
  }
}
