package com.askme.mandelbrot.handler.search.cantorish

import akka.actor.Props
import com.askme.mandelbrot.handler._
import com.askme.mandelbrot.handler.message.IndexParams
import com.askme.mandelbrot.handler.search.cantorish.message._
import spray.http.MediaTypes._
import spray.http.{HttpRequest, RemoteAddress}

/**
 * Created by adichad on 31/03/15.
 */
case object CantorishSearchRouter extends Router {

  override def apply(implicit service: MandelbrotHandler) = {
    import service._
    clientIP { (clip: RemoteAddress) =>
      requestInstance { (httpReq: HttpRequest) =>
        jsonpWithParameter("callback") {
          path("search" / Segment / "cantorish") { (index) =>
            parameters('kw.as[String] ? "", 'city ? "",
              'category_id ? "", 'product_id.as[Int] ? 0, 'variant_id.as[Int] ? 0, 'subscription_id.as[String] ? "",
              'size.as[Int] ? 20, 'offset.as[Int] ? 0,
              'subscriptions_size.as[Int] ? 20, 'subscriptions_offset.as[Int] ? 0,
              'explain.as[Boolean] ? false, 'select ? "id,name",
              'sort.as[String]?"popularity",
              'agg.as[Boolean] ? true,
              'suggest.as[Boolean] ? false,
              'seller_id.as[Int]?0,
              'brand.as[String]?"",
              'product_active_only?false, 'variant_active_only?false, 'subscription_active_only?false,
              'seller_active_only?false) { (kw, city,
               category, product_id, variant_id, subscription_id,
               size, offset,
               subscriptions_size, subscriptions_offset,
               explain, select, sort,
               agg, suggest, seller_id, brand,
               base_active_only, variant_active_only, subscription_active_only, seller_active_only) =>
              val maxdocspershard = 200000
              val searchType = "dfs_query_then_fetch"
              val timeoutms = 1000l
              val aggbuckets = 10
              val source = true

              respondWithMediaType(`application/json`) { ctx =>
                context.actorOf(Props(classOf[CantorishSearchRequestCompleter], config, serverContext, ctx,
                  ProductSearchParams(
                    RequestParams(httpReq, clip, ""),
                    IndexParams(index, "cantorish"),
                    TextParams(kw, suggest),
                    FilterParams(category, product_id, variant_id, subscription_id, city, seller_id, brand,
                      base_active_only, variant_active_only, subscription_active_only, seller_active_only),
                    PageParams(sort, size, offset, subscriptions_size, subscriptions_offset),
                    ViewParams(source, agg, aggbuckets, explain, select, searchType, 1),
                    LimitParams(maxdocspershard, timeoutms),
                    System.currentTimeMillis
                  )
                ))
              }

            }
          }
        }
      }
    }
  }

}
