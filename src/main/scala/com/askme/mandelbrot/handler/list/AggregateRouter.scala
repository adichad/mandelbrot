package com.askme.mandelbrot.handler.list

import akka.actor.Props
import com.askme.mandelbrot.handler.list.message.{AggregateFilterParams, AggregateParams}
import com.askme.mandelbrot.handler.message.IndexParams
import com.askme.mandelbrot.handler.search.message.{LimitParams, PageParams}
import com.askme.mandelbrot.handler.{RequestParams, MandelbrotHandler, Router}
import spray.http.HttpRequest
import spray.http.MediaTypes._

/**
 * Created by adichad on 31/03/15.
 */
case object AggregateRouter extends Router {

  override def apply(implicit service: MandelbrotHandler) = {
    import service._

    clientIP { clip =>
      requestInstance { (httpReq: HttpRequest) =>
        jsonpWithParameter("callback") {
          path("aggregate" / Segment / Segment) { (index, esType) =>
            parameters(
              'city ? "", 'area ? "", 'category ? "",
              'size.as[Int] ? 1000, 'offset.as[Int] ? 0,
              'agg.as[String] ? "City",
              'maxdocspershard.as[Int] ? 500000,
              'timeoutms.as[Long] ? 3000l,
              'searchtype.as[String] ? "count", 'client_ip.as[String] ? "") {
              (city, area, category,
               size, offset,
               agg,
               maxdocspershard,
               timeoutms, searchType, trueClient) =>
                entity(as[String]) { data =>
                  respondWithMediaType(
                    `application/json`) {
                    ctx =>
                      context.actorOf(Props(classOf[AggregateRequestCompleter], config, serverContext, ctx, AggregateParams(
                        RequestParams(httpReq, clip, trueClient),
                        IndexParams(index, esType),
                        AggregateFilterParams(city, area, category, agg),
                        PageParams(size, offset),
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
}
