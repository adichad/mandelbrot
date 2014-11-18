package com.askme.mandelbrot.handler

import java.io.IOException
import java.nio.file.Paths

import akka.actor.SupervisorStrategy.{Escalate, Restart, Resume}
import akka.actor.{Actor, OneForOneStrategy, Props}
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.loader.{FileSystemWatcher, MonitorDir}
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import net.maffoo.jsonquote.literal._

import org.elasticsearch.action.search.{SearchType, SearchRequest}
import org.elasticsearch.common.geo.GeoDistance
import org.elasticsearch.common.unit.{DistanceUnit, Fuzziness}
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.index.query.FilterBuilders._
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.sort
import org.elasticsearch.search.sort._
import spray.http.MediaTypes.`application/json`
import spray.routing.Directive.pimpApply
import spray.routing.{PathMatchers, HttpService}
import spray.routing.PathMatchers._
import spray.json._
import scala.concurrent.future

import scala.concurrent.duration._


class MandelbrotHandler(val config: Config, serverContext: SearchContext) extends HttpService with Actor with Logging with Configurable {

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _: IOException ⇒ Resume
      case _: NullPointerException ⇒ Resume
      case _: Exception ⇒ Restart
    }

  private val fsActor = context.actorOf(Props(classOf[FileSystemWatcher], config, serverContext))

  private val esClient = serverContext.esClient
  private val route =
    path("search" / Segment / Segment) { (index, esType) =>
      get {
        parameters('kw, 'city ? "", 'area ? "", 'size.as[Int] ? 20, 'offset.as[Int] ? 0, 'lat.as[Int]?0, 'lon.as[Int]?0, 'dist.as[Int]?10) {
          (kw, city, area, size, offset, lat, lon, dist) =>
          respondWithMediaType(`application/json`) {
            complete {
              implicit val execctx = serverContext.userExecutionContext
              future {
                val dismax = disMaxQuery()
                kw.split("""\s+""").foreach { w =>
                  dismax
                    .add(termQuery("LocationName", w).boost(10))
                    .add(nestedQuery("Company", termQuery("Company.name", w)).boost(20))
                    //.add(nestedQuery("Company", termQuery("Company.description", w)).boost(0.001f))
                    .add(nestedQuery("StringAttribute", fuzzyQuery("StringAttribute.answer", w).prefixLength(3).fuzziness(Fuzziness.fromSimilarity(0.8f))).boost(25))
                    .add(nestedQuery("StringAttribute", fuzzyQuery("StringAttribute.question", w).prefixLength(3).fuzziness(Fuzziness.fromSimilarity(0.8f))).boost(10))

                    .add(fuzzyQuery("L3Category", w).prefixLength(3).fuzziness(Fuzziness.fromSimilarity(0.8f)).boost(30))
                    .add(fuzzyQuery("L2Category", w).prefixLength(3).fuzziness(Fuzziness.fromSimilarity(0.8f)).boost(5))
                    .add(fuzzyQuery("L1Category", w).prefixLength(3).fuzziness(Fuzziness.fromSimilarity(0.8f)).boost(2))
                    .add(fuzzyQuery("CategoryKeywords", w).prefixLength(3).fuzziness(Fuzziness.fromSimilarity(0.8f)).boost(30))
                }
                val query = boolQuery().must(dismax)
                if(city!="")
                  query.must(termsQuery("City", city.split("""\s+""")))
                if(area!="")
                  query.must(disMaxQuery().add(termsQuery("Area", city.split("""\s+""")))
                    .add(termsQuery("AreaSynonyms", city.split("""\s+"""))))

                val search = esClient.prepareSearch(index.split(","): _*)
                  .setTypes(esType.split(","): _*)
                  .setSearchType(SearchType.QUERY_AND_FETCH)
                  .setQuery(query)
                  .setTrackScores(true)
                  .addSort(new ScoreSortBuilder().order(SortOrder.DESC))
                  .addSort(new FieldSortBuilder("CustomerType").order(SortOrder.DESC))
                  .addSort(new FieldSortBuilder("PPCOnline").order(SortOrder.DESC))
                  .addFields("LocationName",
                    "LocationDescription",
                    "BusinessType",
                    "L3Category",
                    "L2Category","L1Category","CategoryKeywords",
                    "City", "Area")
                  .setFrom(offset).setSize(size)

                if(lat!=0 || lon!=0)
                  search.setPostFilter(
                    geoDistanceFilter("LatLong")
                      .point(lat, lon)
                      .distance(dist, DistanceUnit.KILOMETERS)
                      .optimizeBbox("indexed")
                      .geoDistance(GeoDistance.ARC).cache(true))

                /*search.addAggregation(
                  nested("attributes").path("StringAttribute")
                    .subAggregation(terms("StringAttribute.qaggr").include("").size(10))
                    .subAggregation(terms("StringAttribute.aaggr").size(10)))
                search.addAggregation(terms("L3CategoryID"))*/
                info(s"query [${search.toString()}]")

                search.execute().actionGet().toString()
              }
            }
          }
        }
      }
    } ~
      path("watch") {
        post {
          parameters('dir, 'index, 'type) { (dir, index, esType) =>
            fsActor ! MonitorDir(Paths.get(dir), index, esType)
            respondWithMediaType(`application/json`) {
              complete {
                json"""{
                "acknowledged": true
              }""".toString
              }
            }
          }
        }
      }

  override def receive = {
    runRoute(route)
  }

  override def postStop = {
    info("Kill Message received")
  }

  override def actorRefFactory = context
}
