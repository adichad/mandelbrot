package com.askme.mandelbrot.handler.aggregate

import akka.actor.Props
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.aggregate.message._
import com.askme.mandelbrot.handler.message.IndexParams
import com.askme.mandelbrot.handler.search.message.LimitParams
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
                'city ? "", 'loc ? "", 'cat ? "", 'question ? "", 'answer ? "",
                'size.as[String] ? "1000", 'offset.as[String] ? "0",
                'agg.as[String] ? "city",
                'maxdocspershard.as[Int] ? 500000,
                'timeoutms.as[Long] ? 30000l,
                'searchtype.as[String] ? "count", 'client_ip.as[String] ? "", 'response.as[String] ? "processed") {
                (city, area, category, question, answer,
                 size, offset,
                 agg,
                 maxdocspershard,
                 timeoutms, searchType, trueClient, response) =>
                  entity(as[String]) { data =>
                    respondWithMediaType(
                      `application/json`) {
                      ctx =>
                        val aggSpecs = size.split(",").map(_.trim.toInt).zip(
                          offset.split(",").map(_.trim.toInt)).zip(
                            agg.split(",").map(_.trim)
                          ).map(x=>AggSpec(x._2, x._1._2, x._1._1))

                        context.actorOf(Props(classOf[AggregateRequestCompleter], service.config, serverContext, ctx, AggregateParams(
                          RequestParams(httpReq, clip, trueClient),
                          IndexParams(index, esType),
                          FilterParams(city, area, category, question, answer),
                          AggParams(aggSpecs, response),
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
