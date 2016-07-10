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
          parameters('what.as[String] ? "", 'suggest ? false, 'city ? "", 'area ? "", 'id ? "",
            'applicableto ? "", 'agg ? true, 'size ? 20, 'offset ? 0,
            'select ? "", 'pay_merchant_id ? "", 'edms_outlet_id.as[Int]? 0,
            'gll_outlet_id.as[Int]? 0,
            'category ? "", 'featured ? false, 'dealsource ? "", 'pay_type.as[Int]?0, "explain".as[Boolean]?false)
          { (kw, suggest, city, area, id, applicableTo, agg, size, offset, select,
             pay_merchant_id, edms_outlet_id, gll_outlet_id, category, featured, dealsource, pay_type, explain) =>
            val source = true
            val version = 1
            val fuzzyprefix = 2
            val fuzzysim = 1f
            val maxdocspershard = 30000
            val sort = "_distance,_score"
            val unselect = ""
            val searchType = "query_then_fetch"
            val timeoutms = 600l
            val aggbuckets = 10
            respondWithMediaType(`application/json`) {
               ctx => context.actorOf(Props(classOf[DealSearchRequestCompleter], config, serverContext, ctx, DealSearchParams(
                  req = RequestParams(httpReq, clip, clip.toString()),
                  idx = IndexParams("askmedeal", "deal"),
                  text = TextParams(kw, suggest, fuzzyprefix, fuzzysim),
                  geo = GeoParams(city, area, "", 0.0d, 0.0d),
                  filters = DealFilterParams(id, applicableTo, category, featured, dealsource, pay_merchant_id, edms_outlet_id, gll_outlet_id, pay_type), page = PageParams(size, offset),
                  view = ViewParams(source, agg, aggbuckets, explain, select, unselect, searchType, collapse = false, goldcollapse = false, randomize=false, version),
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
