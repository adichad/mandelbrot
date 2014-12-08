package com.askme.mandelbrot.handler

import java.io.IOException
import java.nio.file.Paths
import java.util

import akka.actor.SupervisorStrategy.{Escalate, Restart, Resume}
import akka.actor.{Actor, OneForOneStrategy, Props}
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.loader.{FileSystemWatcher, MonitorDir}
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.common.xcontent.XContentFactory._
import net.maffoo.jsonquote.literal._


import org.elasticsearch.action.search.{SearchRequestBuilder, SearchType, SearchRequest}
import org.elasticsearch.common.geo.GeoDistance
import org.elasticsearch.common.unit.{DistanceUnit, Fuzziness}
import org.elasticsearch.common.xcontent.{XContentType, XContentFactory, XContentBuilder}
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.index.query.FilterBuilders._
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.sort._
import spray.http.MediaTypes.`application/json`
import spray.routing.Directive.pimpApply
import spray.routing.{HttpService}
import scala.concurrent.future
import scala.concurrent.duration._
import scala.collection.JavaConversions._
import org.json4s._
import org.json4s.jackson.JsonMethods._


class MandelbrotHandler(val config: Config, serverContext: SearchContext) extends HttpService with Actor with Logging with Configurable {

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _: IOException ⇒ Resume
      case _: NullPointerException ⇒ Resume
      case _: Exception ⇒ Restart
    }

  private val fsActor = context.actorOf(Props(classOf[FileSystemWatcher], config, serverContext))

  private val esClient = serverContext.esClient

  private def addSort(search: SearchRequestBuilder, sort: String): Unit = {
    val parts = for (x <- sort.split(",")) yield x.trim
    parts.foreach {
      _ match {
        case "_score" => search.addSort(new ScoreSortBuilder().order(SortOrder.DESC))
        case x => {
          val pair = x.split( """\.""", 2)
          if (pair.size == 2)
            search.addSort(new FieldSortBuilder(pair(0)).order(SortOrder.valueOf(pair(1))))
          else if (pair.size == 1)
            search.addSort(new FieldSortBuilder(pair(0)).order(SortOrder.DESC))
        }
      }
    }
  }

  private val route =
    clientIP { clip =>
      requestInstance { httpReq =>
        get {
          path("search" / Segment / Segment) { (index, esType) =>
            parameters('kw, 'city ? "", 'area ? "",
              'size.as[Int] ? 20, 'offset.as[Int] ? 0,
              'lat.as[Double] ? 0.0d, 'lon.as[Double] ? 0.0d, 'dist.as[Double] ? 10.0d,
              'source.as[Boolean] ? true, 'explain.as[Boolean] ? false,
              'fuzzyprefix.as[Int] ? 3, 'fuzzysim.as[Float] ? 0.8f,
              'sort ? "_score",
              'select ? "_id",
              'agg.as[Boolean] ? true) {
              (kw, city, area,
               size, offset,
               lat, lon, dist,
               source, explain,
               fuzzyprefix, fuzzysim,
               sort,
               select,
               agg) =>

                respondWithMediaType(`application/json`) {
                  complete {
                    implicit val execctx = serverContext.userExecutionContext
                    future {
                      val dismax = disMaxQuery()
                      kw.split( """\s+""").foreach { w =>
                        dismax
                          .add(termQuery("LocationName", w).boost(10))
                          .add(termQuery("CompanyName", w).boost(20))
                          .add(termQuery("CompanyDescription", w).boost(0.001f))
                          .add(nestedQuery("Product", nestedQuery("Product.stringattribute", fuzzyQuery("Product.stringattribute.answer", w).prefixLength(fuzzyprefix).fuzziness(Fuzziness.fromSimilarity(fuzzysim))).boost(25)))
                          .add(nestedQuery("Product", nestedQuery("Product.stringattribute", fuzzyQuery("Product.stringattribute.question", w).prefixLength(fuzzyprefix).fuzziness(Fuzziness.fromSimilarity(fuzzysim))).boost(10)))

                          .add(nestedQuery("Product", fuzzyQuery("Product.l3category", w).prefixLength(fuzzyprefix).fuzziness(Fuzziness.fromSimilarity(fuzzysim)).boost(30)))
                          .add(nestedQuery("Product", fuzzyQuery("Product.l2category", w).prefixLength(fuzzyprefix).fuzziness(Fuzziness.fromSimilarity(fuzzysim)).boost(5)))
                          .add(nestedQuery("Product", fuzzyQuery("Product.l1category", w).prefixLength(fuzzyprefix).fuzziness(Fuzziness.fromSimilarity(fuzzysim)).boost(2)))
                          .add(nestedQuery("Product", fuzzyQuery("Product.categorykeywords", w).prefixLength(fuzzyprefix).fuzziness(Fuzziness.fromSimilarity(fuzzysim)).boost(30)))
                      }
                      val query = boolQuery().must(dismax)
                      if (city != "")
                        query.must(termsQuery("City", city.split( """\s+"""): _*))
                      if (area != "")
                        query.must(disMaxQuery().add(termsQuery("Area", area.split( """\s+"""): _*))
                          .add(termsQuery("AreaSynonyms", area.split( """\s+"""): _*)))

                      val search = esClient.prepareSearch(index.split(","): _*)
                        .setTypes(esType.split(","): _*)
                        .setSearchType(SearchType.QUERY_THEN_FETCH)
                        .setQuery(query)
                        .setTrackScores(true)
                        .addFields(select.split( ""","""): _*)
                        .setFrom(offset).setSize(size)
                      addSort(search, sort)

                      if (lat != 0.0d || lon != 0.0d)
                        search.setPostFilter(
                          geoDistanceFilter("LatLong")
                            .point(lat, lon)
                            .distance(dist, DistanceUnit.KILOMETERS)
                            .optimizeBbox("indexed")
                            .geoDistance(GeoDistance.ARC).cache(true))

                      search.setFetchSource(source)

                      if (agg) {
                        search.addAggregation(
                          terms("city").field("City")
                        )
                        search.addAggregation(nested("products").path("Product")
                          .subAggregation(terms("categories").field("Product.cat3aggr"))
                          .subAggregation(nested("attributes").path("Product.stringattribute")
                            .subAggregation(terms("questions").field("Product.stringattribute.qaggr")
                              .subAggregation(terms("answers").field("Product.stringattribute.aaggr"))
                            )

                          )
                        )
                      }
                      info(s"query [${pretty(render(parse(search.toString)))}]")

                      val result = search.execute().actionGet()
                      info(httpReq.uri + ": " + clip.toString + ": " + result.getTook)
                      result.toString()
                    }
                  }
                }
            }
          }
        } ~
          post {
            path("watch") {
              anyParams('dir, 'index, 'type) { (dir, index, esType) =>
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
