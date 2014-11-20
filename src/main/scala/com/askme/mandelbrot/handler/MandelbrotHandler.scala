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
import org.elasticsearch.search.sort
import org.elasticsearch.search.sort._
import spray.http.MediaTypes.`application/json`
import spray.routing.Directive.pimpApply
import spray.routing.{PathMatchers, HttpService}
import spray.routing.PathMatchers._
import spray.json._
import scala.concurrent.future

import scala.concurrent.duration._
import scala.collection.JavaConversions._


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
    val parts = for(x <- sort.split(",")) yield x.trim
    parts.foreach {
      _ match {
        case "_score" => search.addSort(new ScoreSortBuilder().order(SortOrder.DESC))
        case x => {
          val pair = x.split("""\.""", 2)
          if(pair.size==2)
            search.addSort(new FieldSortBuilder(pair(0)).order(SortOrder.valueOf(pair(1))))
          else if(pair.size==1)
            search.addSort(new FieldSortBuilder(pair(0)).order(SortOrder.DESC))
        }
      }
    }
  }

  private val route =
    get {
      path("search" / Segment / Segment) { (index, esType) =>
        parameters('kw, 'city ? "", 'area ? "",
          'size.as[Int] ? 20, 'offset.as[Int] ? 0,
          'lat.as[Int]?0, 'lon.as[Int]?0, 'dist.as[Int]?10,
          'source.as[Boolean]?true, 'explain.as[Boolean]?false,
          'fuzzyprefix.as[Int]?3, 'fuzzysim.as[Float]?0.8f,
          'sort?"_score",
          'select?"_id",
          'agg.as[Boolean]?false) {
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
                  kw.split("""\s+""").foreach { w =>
                    dismax
                      .add(termQuery("LocationName", w).boost(10))
                      .add(nestedQuery("Company", termQuery("Company.name", w)).boost(20))
                      .add(nestedQuery("Company", termQuery("Company.description", w)).boost(0.001f))
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
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setQuery(query)
                    .setTrackScores(true)
                    .addFields(select.split(""","""):_*)
                    .setFrom(offset).setSize(size)
                  addSort(search, sort)

                  if(lat!=0 || lon!=0)
                    search.setPostFilter(
                      geoDistanceFilter("LatLong")
                        .point(lat, lon)
                        .distance(dist, DistanceUnit.KILOMETERS)
                        .optimizeBbox("indexed")
                        .geoDistance(GeoDistance.ARC).cache(true))

                  val extraSource = new util.HashMap[String, Any]()
                  extraSource.put("_source", source)

                  if(agg) {
                    extraSource.put("aggregations",
                      new util.HashMap[String, Any] {{
                        put("attributes", new util.HashMap[String, Any] {{
                          put("nested", new util.HashMap[String, Any] {{
                            put("path", "StringAttribute")
                          }})
                          put("aggregations", new util.HashMap[String, Any] {{
                            put("questions", new util.HashMap[String, Any] {{
                              put("terms", new util.HashMap[String, Any] {{
                                put("field", "StringAttribute.qaggr")
                              }})
                              put("aggregations", new util.HashMap[String, Any] {{
                                put("answers", new util.HashMap[String, Any] {{
                                  put("terms", new util.HashMap[String, Any] {{
                                    put("field", "StringAttribute.aaggr")
                                  }})
                                }})
                              }})
                            }})
                          }})
                        }})
                        put("categories", new util.HashMap[String, Any] {{
                          put("terms", new util.HashMap[String, Any] {{
                            put("field", "L3CategoryAggr")
                            put("shard_size", 0)
                          }})
                        }})
                      }}
                    )
                  }
                  search.setExtraSource(jsonBuilder.map(extraSource))
                  info(s"query [${search.toString()}]")

                  search.execute().actionGet().toString()
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

  override def receive = {
    runRoute(route)
  }

  override def postStop = {
    info("Kill Message received")
  }

  override def actorRefFactory = context
}
