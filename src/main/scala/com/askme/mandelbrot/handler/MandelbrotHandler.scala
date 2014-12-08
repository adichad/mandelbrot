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
import org.elasticsearch.index.query.BaseQueryBuilder
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

  private def nestIfNeeded(fieldName: String, q: BaseQueryBuilder): BaseQueryBuilder = {
    val parts = fieldName.split(".")
    var res = q
    (1 to parts.length-1).foreach { until =>
      res = nestedQuery(parts.slice(0, until).mkString("."), q)
    }
    res
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
              'fuzzyprefix.as[Int] ? 3, 'fuzzysim.as[Float] ? 0.85f,
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

                      val query = boolQuery()
                      if(kw!=null && kw.trim!="") {
                        val kwquery = boolQuery()
                        val w = kw.split( """\s+""")
                        val searchFields = Map("LocationName" -> 10, "CompanyName" -> 20, "Product.l3category" -> 40,
                          "Product.categorykeywords" -> 40, "Product.l2category" -> 5, "Product.l1category" -> 2)


                        searchFields.foreach {
                          field: (String, Int) => {
                            val fieldQuery = spanOrQuery().boost(field._2)
                            val locNameTerms = w
                              .map(fuzzyQuery(field._1, _).prefixLength(fuzzyprefix).fuzziness(Fuzziness.fromSimilarity(fuzzysim)))
                              .map(spanMultiTermQueryBuilder)
                            (1 to Math.min(locNameTerms.length, 4)).foreach { len =>
                              locNameTerms.sliding(len).foreach { shingle =>
                                val nearQuery = spanNearQuery().slop(shingle.length - 1).inOrder(false).boost(field._2 * shingle.length)
                                shingle.foreach(nearQuery.clause)
                                fieldQuery.clause(nearQuery)
                              }
                            }
                            kwquery.should(nestIfNeeded(field._1, fieldQuery))

                          }
                        }
                        query.must(kwquery)
                      }

                      if (city != "")
                        query.must(termsQuery("City", city.split( ""","""): _*))
                      if (area != "")
                        query.must(
                          disMaxQuery().add(termsQuery("Area", area.split( """\s+"""): _*))
                            .add(termsQuery("AreaSynonyms", area.split( """\s+"""): _*))
                        )

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
