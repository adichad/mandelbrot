package com.askme.mandelbrot.handler.list

import akka.actor.Props
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.list.message.{AggregateFilterParams, AggregateParams}
import com.askme.mandelbrot.handler.message.IndexParams
import com.askme.mandelbrot.handler.search.message.{LimitParams, PageParams}
import com.askme.mandelbrot.handler.{RequestParams, MandelbrotHandler, Router}
import com.typesafe.config.Config
import spray.http.{StatusCodes, HttpRequest}
import spray.http.MediaTypes._

/**
 * Created by adichad on 31/03/15.
 */
case class AggregateRouter(val config: Config) extends Router with Configurable {

  override def apply(implicit service: MandelbrotHandler) = {
    import service._

    clientIP { clip =>
      requestInstance { (httpReq: HttpRequest) =>
        jsonpWithParameter("callback") {
          path("aggregate" / Segment / Segment) { (index, esType) =>
            if(boolean("enabled")) {
              parameters(
                'city ? "", 'loc ? "", 'cat ? "",
                'size.as[Int] ? 1000, 'offset.as[Int] ? 0,
                'agg.as[String] ? "city",
                'maxdocspershard.as[Int] ? 500000,
                'timeoutms.as[Long] ? 5000l,
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
                        context.actorOf(Props(classOf[AggregateRequestCompleter], service.config, serverContext, ctx, AggregateParams(
                          RequestParams(httpReq, clip, trueClient),
                          IndexParams(index, esType),
                          AggregateFilterParams(city, area, category,
                            if (agg.toLowerCase == "city") "CityAggr"
                            else if (agg.toLowerCase == "loc") "AreaAggr"
                            else if (agg.toLowerCase == "cat") "Product.l3categoryaggr"
                            else agg),
                          PageParams(size, offset),
                          LimitParams(maxdocspershard, timeoutms),
                          System.currentTimeMillis
                        )))


                    }
                  }
              }
            } else {
              respondWithMediaType(`application/json`) {
                complete(StatusCodes.MethodNotAllowed, "unsupported operation")
              }
            }
          }
        }
      }
    }
  }
}
