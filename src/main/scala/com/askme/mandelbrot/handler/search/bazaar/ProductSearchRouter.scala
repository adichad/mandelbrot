package com.askme.mandelbrot.handler.search.bazaar

import akka.actor.Props
import com.askme.mandelbrot.handler._
import com.askme.mandelbrot.handler.message.IndexParams
import com.askme.mandelbrot.handler.search.bazaar.message._
import com.askme.mandelbrot.util.Utils._
import spray.http.MediaTypes._
import spray.http.{HttpRequest, RemoteAddress}

/**
  * Created by adichad on 31/03/15.
  */
case object ProductSearchRouter extends Router {

  override def apply(implicit service: MandelbrotHandler) = {
    import service._
    clientIP { (clip: RemoteAddress) =>
      requestInstance { (httpReq: HttpRequest) =>
        jsonpWithParameter("callback") {
          path("search" / Segment / "product") { (index) =>
            parameterMap {
              params =>
                val kw = params.getOrElse("kw", "")
                val city = params.getOrElse("city", "")
                val national_only = params.getOrElse("national_only", "false").toBoolean
                val ndd_only = params.getOrElse("ndd_only", "false").toBoolean
                val category = params.getOrElse("category", "")
                val category_id = params.getOrElse("category_id", "0").toInt
                val brand = params.getOrElse("brand", "")
                val product_id = params.getOrElse("product_id", "0").toInt
                val grouped_id = params.getOrElse("grouped_id", "0").toInt
                val base_id = params.getOrElse("base_id", "0").toInt
                val subscribed_id = params.getOrElse("subscribed_id", "0").split(""",""").map(_.toInt).filter(_>0)
                val size = params.getOrElse("size", "20").toInt
                val offset = params.getOrElse("offset", "0").toInt
                val subscriptions_size = params.getOrElse("subscriptions_size", "20").toInt
                val subscriptions_offset = params.getOrElse("subscriptions_offset", "0").toInt
                val explain = params.getOrElse("explain", "false").toBoolean
                val select = params.getOrElse("select", "product_id,name")
                val sort = params.getOrElse("sort", "popularity")
                val store = params.getOrElse("store", "")
                val agg = params.getOrElse("agg", "true").toBoolean
                val suggest = params.getOrElse("suggest", "false").toBoolean
                val store_front_id = params.getOrElse("store_front_id", "0").toInt
                val mpdm_store_front_id = params.getOrElse("mpdm_store_front_id", "0").toInt
                val crm_seller_id = params.getOrElse("crm_seller_id", "0").toInt
                val price_min = params.getOrElse("price_min", "0").toFloat
                val price_max = params.getOrElse("price_max", "0").toFloat
                val filters = params.filterKeys(_.startsWith("Filter_"))
                val optionFilters = params.filterKeys(_.startsWith("OptionFilter_"))
                  .map(x=>(x._1.substring("OptionFilter_".length), x._2))
                  .filter(_._1.nonEmpty)

                val maxdocspershard = 200000
                val searchType = "dfs_query_then_fetch"
                val timeoutms = 1000l
                val aggbuckets = 100
                val source = true

                respondWithMediaType(`application/json`) { ctx =>
                  context.actorOf(Props(classOf[ProductSearchRequestCompleter], parentPath, serverContext, ctx,
                    ProductSearchParams(
                      RequestParams(httpReq, clip, ""),
                      IndexParams(index, "product"),
                      TextParams(kw, suggest),
                      FilterParams(category, product_id, grouped_id, base_id, subscribed_id, store, city,
                        national_only, ndd_only,
                        store_front_id, mpdm_store_front_id, crm_seller_id, brand, filters, optionFilters,
                        price_min, price_max, category_id),
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
