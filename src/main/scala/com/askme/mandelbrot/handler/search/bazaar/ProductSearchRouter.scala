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
            parameters('kw.as[String] ? "", 'city ? "",
              'category ? "", 'product_id.as[Int] ? 0, 'grouped_id.as[Int] ? 0, 'base_id.as[Int] ? 0, 'subscribed_id.as[Int] ? 0,
              'size.as[Int] ? 20, 'offset.as[Int] ? 0,
              'explain.as[Boolean] ? false, 'select ? "product_id,name",
              'sort.as[String]?"popularity",
              'store.as[String]?"",
              'agg.as[Boolean] ? true,
              'version.as[Int] ? 1,
              'client_ip.as[String] ? "") { (kw, city,
               category, product_id, grouped_id, base_id, subscribed_id,
               size, offset,
               explain, select, sort, store,
               agg, version,
               trueClient) =>
              val maxdocspershard = 5000
              val searchType = "dfs_query_then_fetch"
              val timeoutms = 600l
              val aggbuckets = 10
              val source = true

              respondWithMediaType(`application/json`) { ctx =>
                context.actorOf(Props(classOf[ProductSearchRequestCompleter], config, serverContext, ctx,
                  ProductSearchParams(
                    RequestParams(httpReq, clip, trueClient),
                    IndexParams(index, "product"),
                    TextParams(kw.nonEmptyOrElse(category)),
                    FilterParams(category, product_id, grouped_id, base_id, subscribed_id, store, city),
                    PageParams(sort, size, offset),
                    ViewParams(source, agg, aggbuckets, explain, select, searchType, version),
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
