package com.askme.mandelbrot.handler.search

import akka.actor.Props
import com.askme.mandelbrot.handler._
import com.askme.mandelbrot.handler.message.IndexParams
import com.askme.mandelbrot.handler.search.message._
import spray.http.MediaTypes._
import spray.http.{HttpRequest, RemoteAddress}

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
            'applicableto ? "", 'wantaggr ? "no", 'size ? 20, 'offset ? 0,
            'select ? "", 'screentype ? "", 'category ? "", 'featured ? "", 'dealsource ? "")
          { (kw, city, area, id, applicableTo, wantaggrs, size, offset, select,
             screentype, category, featured, dealsource) =>
            val source = true
            val version = 1
            val fuzzyprefix = 2
            val fuzzysim = 1f
            val slugFlag = true
            val maxdocspershard = 30000
            val sort = "_distance,_score"
            val unselect = ""
            val searchType = "query_then_fetch"
            val timeoutms = 600l
            val aggbuckets = 10
            var aggr = false
            if (wantaggrs == "yes") {
              aggr = true
            }
            respondWithMediaType(`application/json`) {
               ctx => context.actorOf(Props(classOf[DealSearchRequestCompleter], config, serverContext, ctx, DealSearchParams(
                  req = RequestParams(httpReq, clip, clip.toString()),
                  idx = IndexParams("askmedeal", "deal"),
                  text = TextParams(kw, fuzzyprefix, fuzzysim),
                  geo = GeoParams(city, area, "", 0.0d, 0.0d, 0d, 20.0d),
                  filters = DealFilterParams(id, applicableTo, screentype, category, featured, dealsource), page = PageParams(size, offset),
                  view = ViewParams(source, aggr, aggbuckets, explain = false, select, unselect, searchType, slugFlag = slugFlag, collapse = false, goldcollapse = false, version),
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
