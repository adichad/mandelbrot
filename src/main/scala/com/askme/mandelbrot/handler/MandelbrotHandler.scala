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


import org.elasticsearch.action.search.{SearchRequestBuilder, SearchType, SearchRequest}
import org.elasticsearch.common.geo.GeoDistance
import org.elasticsearch.common.unit.{TimeValue, DistanceUnit, Fuzziness}
import org.elasticsearch.index.query.{QueryBuilders, BaseQueryBuilder}
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.index.query.FilterBuilders._
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket
import org.elasticsearch.search.aggregations.{AggregationBuilders, Aggregation, Aggregations}
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation
import org.elasticsearch.search.aggregations.bucket.nested.Nested
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.sort._
import spray.http.MediaTypes.`application/json`
import spray.routing.Directive.pimpApply
import spray.routing.HttpService
import scala.concurrent.future
import scala.concurrent.duration._
import scala.collection.JavaConversions._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.net.URLEncoder


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
            parameters('kw.as[String] ? "", 'city ? "", 'area ? "", 'category ? "",
              'size.as[Int] ? 20, 'offset.as[Int] ? 0,
              'lat.as[Double] ? 0.0d, 'lon.as[Double] ? 0.0d, 'fromkm.as[Double]? 0d, 'tokm.as[Double] ? 20.0d,
              'source.as[Boolean] ? true, 'explain.as[Boolean] ? false,
              'fuzzyprefix.as[Int] ? 3, 'fuzzysim.as[Float] ? 0.85f,
              'sort ? "CustomerType.DESC,_score",
              'select ? "_id",
              'agg.as[Boolean] ? true,
              'maxdocspershard.as[Int]?100000,
              'timeoutms.as[Long]?2000l) {
              (kw, city, area, category,
               size, offset,
               lat, lon, fromkm, tokm,
               source, explain,
               fuzzyprefix, fuzzysim,
               sort,
               select,
               agg,
               maxdocspershard,
               timeoutms) =>

                respondWithMediaType(`application/json`) {
                  complete {
                    implicit val execctx = serverContext.userExecutionContext
                    future {
                      var query: BaseQueryBuilder = null
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
                                val nearQuery = spanNearQuery.slop(shingle.length - 1).inOrder(false).boost(field._2 * shingle.length)
                                shingle.foreach(nearQuery.clause)
                                fieldQuery.clause(nearQuery)
                              }
                            }
                            kwquery.should(nestIfNeeded(field._1, fieldQuery))
                          }
                        }

                        val exactFields = Map("Product.l3categoryexact"->80, "Product.categorykeywordsexact"->80)
                        exactFields.foreach {
                          field: (String, Int) => {
                            val fieldQuery = boolQuery
                            (1 to Math.min(w.length, 4)).foreach { len =>
                              w.sliding(len).foreach { shingle =>
                                fieldQuery.should(termQuery(field._1, shingle.mkString(" ").toLowerCase).boost(field._2 * shingle.length))
                              }
                            }
                            kwquery.should(nestIfNeeded(field._1, fieldQuery))
                          }
                        }
                        query = kwquery
                      }

                      // filters
                      if (city != "")
                        query = filteredQuery(query, termsFilter("City", city.split( """,""").map(_.trim.toLowerCase): _*).cache(true))
                      if (area != "") {
                        query = filteredQuery(query, boolFilter
                          .should(termsFilter("AreaExact", area.split( """,""").map(_.trim.toLowerCase): _*))
                          .should(termsFilter("AreaSynonymsExact", area.split( """,""").map(_.trim.toLowerCase): _*)).cache(true)
                        )
                      }
                      if (category != "") {
                        query = filteredQuery(query, nestedFilter("Product", termsFilter("Product.l3categoryexact", category.split("""#""").map(_.trim.toLowerCase): _*)).cache(true))
                      }
                      if (lat != 0.0d || lon != 0.0d)
                        query = filteredQuery(query,
                          geoDistanceRangeFilter("LatLong")
                            .point(lat, lon)
                            .from(fromkm+"km")
                            .to(tokm+"km")
                            .optimizeBbox("indexed")
                            .geoDistance(GeoDistance.SLOPPY_ARC).cache(true))

                      val search = esClient.prepareSearch(index.split(","): _*)
                        .setTypes(esType.split(","): _*)
                        .setSearchType(SearchType.QUERY_THEN_FETCH)
                        .setQuery(query)
                        .setTrackScores(true)
                        .addFields(select.split( ""","""): _*)
                        .setFrom(offset).setSize(size)
                        .setTimeout(TimeValue.timeValueMillis(Math.min(timeoutms, long("timeoutms"))))
                        .setTerminateAfter(Math.min(maxdocspershard, int("max-docs-per-shard")))
                        .setExplain(explain)
                        .setFetchSource(source)
                      
                      addSort(search, sort)

                      if (agg) {
                        if(city=="")
                          search.addAggregation(terms("city").field("CityAggr"))
                        search.addAggregation(terms("area").field("AreaAggr"))
                        search.addAggregation(nested("products").path("Product")
                            .subAggregation(terms("categories").field("Product.cat3aggr"))
                            .subAggregation(terms("catkw").field("Product.cat3aggr")
                              .subAggregation(terms("kw").field("Product.cat3kwexact"))
                            )
                          /*
                              .subAggregation(nested("attributes").path("Product.stringattribute")
                                .subAggregation(terms("questions").field("Product.stringattribute.qaggr")
                                  .subAggregation(terms("answers").field("Product.stringattribute.aaggr"))
                                )
                              )
                              */
                        )
                        if(lat!=0.0d || lon!=0.0d)
                          search.addAggregation(
                            geoDistance("geotarget")
                              .field("LatLong")
                              .lat(lat).lon(lon)
                              .distanceType(GeoDistance.SLOPPY_ARC)
                              .unit(DistanceUnit.KILOMETERS)
                              .addUnboundedTo("within 1.5 kms", 1.5d)
                              .addRange("1.5 to 4 kms", 1.5d, 4d)
                              .addRange("4 to 8 kms", 4d, 8d)
                              .addRange("8 to 30 kms", 8d, 30d)
                              .addUnboundedFrom("30 kms and beyond", 30d)
                          )


                      }

                      debug("query "+pretty(render(parse(search.toString)))+"]")

                      val result = search.execute().actionGet()

                      val cleanKW = kw.trim.toLowerCase

                      val matchedCat = result.getAggregations.get("products").asInstanceOf[Nested].getAggregations.get("catkw").asInstanceOf[Terms].getBuckets
                        .find(b => b.getKey.trim.toLowerCase == cleanKW || (b.getAggregations.get("kw").asInstanceOf[Terms].getBuckets.exists(_.getKey.trim.toLowerCase == cleanKW)))
                        .fold("/search/" + URLEncoder.encode(cleanKW.replaceAll("""\s+""", "-"), "UTF-8"))(k => "/" + URLEncoder.encode(k.getKey.replaceAll("""\s+""", "-"), "UTF-8"))

                      val slug = (if(city!="") "/" + URLEncoder.encode(city.trim.toLowerCase.replaceAll("""\s+""", "-"), "UTF-8") else "") +
                        matchedCat +
                        (if(category!="") "/cat/" + URLEncoder.encode(category.trim.toLowerCase.replaceAll("""\s+""", "-"), "UTF-8") else "") +
                        (if(area!="") "/in/" + URLEncoder.encode(area.trim.toLowerCase.replaceAll("""\s+""", "-"), "UTF-8") else "")

                      info("[" + clip.toString + "]->[" + httpReq.uri + "]=[" + result.getTook + " (" + result.getHits.hits.length + ")]")

                      "{ \"slug\": \""+slug+"\", \"hit-count\": "+result.getHits.hits.length+", \"results\": "+result.toString+" }"
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
