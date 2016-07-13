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
            parameterMap {
              params =>
                val kw = params.getOrElse("kw", "")
                val zone_code = params.getOrElse("zone_code", "")
                val category = params.getOrElse("category", "")
                val brand = params.getOrElse("brand", "")
                val product_id = params.getOrElse("product_id", "0").toInt
                val variant_id = params.getOrElse("variant_id", "0").toInt
                val geo_id = params.getOrElse("geo_id", "0").toLong
                val item_id = params.getOrElse("item_id", "")
                val size = params.getOrElse("size", "20").toInt
                val offset = params.getOrElse("offset", "0").toInt
                val explain = params.getOrElse("explain", "false").toBoolean
                val select = params.getOrElse("select", "product_id,name")
                val sort = params.getOrElse("sort", "popularity")
                val user_id = params.getOrElse("store", "")
                val agg = params.getOrElse("agg", "true").toBoolean
                val suggest = params.getOrElse("suggest", "true").toBoolean
                val storefront_id = params.getOrElse("storefront_id", "0").toInt
                val order_id = params.getOrElse("order_id", "")
                val parent_order_id = params.getOrElse("parent_order_id", "")
                val order_status = params.getOrElse("order_status", "")
                val order_updated_since = params.getOrElse("order_updated_since", "")
                val order_geo_id = params.getOrElse("order_geo_id", "0").toLong
                val include_inactive_items = params.getOrElse("include_inactive_items", "false").toBoolean
                val extended_agg = params.getOrElse("extended_agg", "false").toBoolean

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
                      FilterParams(category, variant_id, product_id, item_id, storefront_id, geo_id, zone_code, brand,
                        user_id, order_id, parent_order_id, order_status, order_updated_since, order_geo_id, include_inactive_items),
                      PageParams(sort, size, offset),
                      ViewParams(source, agg, aggbuckets, extended_agg, explain, select, searchType, 1),
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
