package com.askme.mandelbrot.handler.search.grocery

import akka.actor.Props
import com.askme.mandelbrot.handler._
import com.askme.mandelbrot.handler.message.IndexParams
import com.askme.mandelbrot.handler.search.grocery.message._
import com.askme.mandelbrot.util.Utils._
import spray.http.MediaTypes._
import spray.http.{HttpRequest, RemoteAddress}

/**
 * Created by adichad on 31/03/15.
 */
case object GrocerySearchRouter extends Router {

  override def apply(implicit service: MandelbrotHandler) = {
    import service._
    clientIP { (clip: RemoteAddress) =>
      requestInstance { (httpReq: HttpRequest) =>
        jsonpWithParameter("callback") {
          path("search" / Segment / "grocery") { (index) =>
            parameters('kw.as[String] ? "", 'zone_code ? "",
              'category ? "", 'variant_id.as[Int] ? 0, 'product_id.as[Int] ? 0, 'item_id.as[String] ? "",
              'storefront_id ? 0, 'geo_id ? 0,
              'size.as[Int] ? 20, 'offset.as[Int] ? 0,
              'explain.as[Boolean] ? false, 'select ? "variant_id,variant_title",
              'sort.as[String]?"popularity",
              'agg.as[Boolean] ? true,
              'suggest.as[Boolean] ? true,
              'brand.as[String]?"") { (kw, zone_code,
               category, variant_id, product_id, item_id,
               storefront_id, geo_id,
               size, offset,
               explain, select, sort,
               agg, suggest, brand) =>
              val maxdocspershard = 10000
              val searchType = "dfs_query_then_fetch"
              val timeoutms = 600l
              val aggbuckets = 10
              val source = true

              respondWithMediaType(`application/json`) { ctx =>
                context.actorOf(Props(classOf[GrocerySearchRequestCompleter], config, serverContext, ctx,
                  GrocerySearchParams(
                    RequestParams(httpReq, clip, ""),
                    IndexParams(index, "grocery"),
                    TextParams(kw, suggest),
                    FilterParams(category, variant_id, product_id, item_id, storefront_id, geo_id, zone_code, brand),
                    PageParams(sort, size, offset),
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
