package com.askme.mandelbrot.handler.search.geo

import akka.actor.Props
import com.askme.mandelbrot.handler._
import com.askme.mandelbrot.handler.message.IndexParams
import com.askme.mandelbrot.handler.search.geo.message._
import spray.http.MediaTypes._
import spray.http.{HttpRequest, RemoteAddress}

/**
 * Created by adichad on 31/03/15.
 */
case object GeoSearchRouter extends Router {

  override def apply(implicit service: MandelbrotHandler) = {
    import service._
    clientIP { (clip: RemoteAddress) =>
      requestInstance { (httpReq: HttpRequest) =>
        jsonpWithParameter("callback") {
          path("search" / Segment / "geo") { (index) =>
            parameters(
              'gids.as[String] ? "", 'kw.as[String] ? "", 'type ? "", 'tag ? "", 'phone_prefix ? "",
              'container_gids.as[String] ? "", 'container ? "", 'container_type ? "", 'container_tag ? "", 'container_phone_prefix ? "",
              'related_gids.as[String] ? "", 'related ? "", 'related_type?"", 'related_tag?"", 'related_phone_prefix ? "",
              'lat.as[Double] ? 0.0d, 'lon.as[Double] ? 0.0d, 'to_km.as[Double] ? 0.0d,
              'size.as[Int] ? 20, 'offset.as[Int] ? 0,
              'explain.as[Boolean] ? false, 'select ? "gid,name") { (gids, kw, `type`, tag, phone_prefix,
                                       container_gids, container, container_type, container_tag, container_phone_prefix,
                                       related_gids, related, related_type, related_tag, related_phone_prefix,
                                       lat, lon, to_km,
                                       size, offset,
                                       explain, select) =>
              val maxdocspershard = 500
              val searchType = "query_then_fetch"
              val timeoutms = 200l
              val aggbuckets = 10
              val source = true
              val version = 1

              respondWithMediaType(`application/json`) { ctx =>
                context.actorOf(Props(classOf[GeoSearchRequestCompleter], config, serverContext, ctx,
                  GeoSearchParams(
                    RequestParams(httpReq, clip, ""),
                    IndexParams(index, "geo"),
                    TextParams(kw),
                    GeoParams(lat, lon, to_km),
                    FilterParams(gids, kw, `type`, tag, phone_prefix),
                    ContainerParams(container_gids, container, container_type, container_tag, container_phone_prefix),
                    RelatedParams(related_gids, related, related_type, related_tag, related_phone_prefix),
                    PageParams(size, offset),
                    ViewParams(source, explain, select, searchType, version),
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
