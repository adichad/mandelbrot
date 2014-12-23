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


import spray.http.{HttpMethods, HttpMethod, HttpResponse, AllOrigins}
import spray.http.HttpHeaders._
import spray.http.HttpMethods._
import spray.routing._

// see also https://developer.mozilla.org/en-US/docs/Web/HTTP/Access_control_CORS
trait CORS {
  this: HttpService =>

  private val allowOriginHeader = `Access-Control-Allow-Origin`(AllOrigins)
  private val optionsCorsHeaders = List(
    `Access-Control-Allow-Headers`("Origin, X-Requested-With, Content-Type, Accept, Accept-Encoding, Accept-Language, Host, Referer, User-Agent"),
    `Access-Control-Max-Age`(1728000))

  def cors[T]: Directive0 = mapRequestContext { ctx => ctx.withRouteResponseHandling({
    //It is an option requeset for a resource that responds to some other method
    case Rejected(x) if (ctx.request.method.equals(HttpMethods.OPTIONS) && !x.filter(_.isInstanceOf[MethodRejection]).isEmpty) => {
      val allowedMethods: List[HttpMethod] = x.filter(_.isInstanceOf[MethodRejection]).map(rejection => {
        rejection.asInstanceOf[MethodRejection].supported
      })
      ctx.complete(HttpResponse().withHeaders(
        `Access-Control-Allow-Methods`(OPTIONS, allowedMethods: _*) :: allowOriginHeader ::
          optionsCorsHeaders
      ))
    }
  }).withHttpResponseHeadersMapped { headers =>
    allowOriginHeader :: headers

  }
  }
}

class MandelbrotHandler(val config: Config, serverContext: SearchContext) extends HttpService with Actor with Logging with Configurable with CORS {

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
    (1 to parts.length - 1).foreach { until =>
      res = nestedQuery(parts.slice(0, until).mkString("."), q)
    }
    res
  }

  private val route =
    cors {
      clientIP { clip =>
        requestInstance { httpReq =>
          get {
            jsonpWithParameter("callback") {
              path("apidocs") {
                respondWithMediaType(`application/json`) {
                  complete {
                    """
                      |{
                      |  "api": "GET /search/<index>/<type>",
                      |  "parameters": {
                      |    "kw": {
                      |      "type": "String",
                      |      "required": false,
                      |      "default": "",
                      |      "description": "free-form 'keywords'/'text' searched in analyzed fields",
                      |      "multivalued": false
                      |    },
                      |    "city": {
                      |      "type": "String",
                      |      "required": false,
                      |      "default": "",
                      |      "description": "filter on 'City' field",
                      |      "multivalued": true,
                      |      "seperator": ","
                      |    },
                      |    "area": {
                      |      "type": "String",
                      |      "required": false,
                      |      "default": "",
                      |      "description": "filter on 'Area', 'AreaSynonyms' fields",
                      |      "multivalued": true,
                      |      "seperator": ","
                      |    },
                      |    "pin": {
                      |      "type": "String",
                      |      "required": false,
                      |      "default": "",
                      |      "description": "filter on 'PinCode' field",
                      |      "multivalued": true,
                      |      "seperator": ","
                      |    },
                      |    "category": {
                      |      "type": "String",
                      |      "required": false,
                      |      "default": "",
                      |      "description": "filter on 'Product.l3categoryexact' field, for use in navigation from the 'categories' aggregation",
                      |      "multivalued": true,
                      |      "seperator": "#"
                      |    },
                      |    "id": {
                      |      "type": "String",
                      |      "required": false,
                      |      "default": "",
                      |      "description": "filter by document ids",
                      |      "multivalued": true,
                      |      "seperator": ","
                      |    },
                      |    "size": {
                      |      "type": "Integer",
                      |      "required": false,
                      |      "default": 20,
                      |      "description": "the number of hits to return",
                      |      "multivalued": false
                      |    },
                      |    "offset": {
                      |      "type": "Integer",
                      |      "required": false,
                      |      "default": 0,
                      |      "description": "the number of hits to skip from the top, used for paging in tandem with 'size'",
                      |      "multivalued": false
                      |    },
                      |    "lat": {
                      |      "type": "Double",
                      |      "required": false,
                      |      "default": 0.0,
                      |      "description": "latitude (degrees) of point around which to focus search",
                      |      "multivalued": false
                      |    },
                      |    "lon": {
                      |      "type": "Double",
                      |      "required": false,
                      |      "default": 0.0,
                      |      "description": "longitude (degrees) of point around which to focus search",
                      |      "multivalued": false
                      |    },
                      |    "fromkm": {
                      |      "type": "Double",
                      |      "required": false,
                      |      "default": 0.0,
                      |      "description": "distance in km from point specified by 'lat','lon' that specifies a lower-bound (inclusive) on the distance filter; for use in navigation from the 'geotarget' aggregation",
                      |      "multivalued": false
                      |    },
                      |    "tokm": {
                      |      "type": "Double",
                      |      "required": false,
                      |      "default": 20.0,
                      |      "description": "distance in km from point specified by 'lat','lon' that specifies an upper-bound (inclusive) on the distance filter; for use in navigation from the 'geotarget' aggregation",
                      |      "multivalued": false
                      |    },
                      |    "select": {
                      |      "type": "String",
                      |      "required": false,
                      |      "default": "_id",
                      |      "description": "list of field values to retrieve for each hit, caveat: nested fields are returned flattened",
                      |      "multivalued": true,
                      |      "seperator": ","
                      |    },
                      |    "agg": {
                      |      "type": "Boolean",
                      |      "required": false,
                      |      "default": true,
                      |      "description": "whether to compute aggregations on the result-set",
                      |      "multivalued": false
                      |    },,
                      |    "aggbuckets": {
                      |      "type": "Integer",
                      |      "required": false,
                      |      "default": 10,
                      |      "description": "number of buckets to return for each aggregation",
                      |      "multivalued": false
                      |    }
                      |    "source": {
                      |      "type": "Boolean",
                      |      "required": false,
                      |      "default": false,
                      |      "description": "whether to include raw _source depicting the indexed document for every result",
                      |      "multivalued": false
                      |    }
                      |  },
                      |  "example": "GET http://138.91.34.100:9999/search/askme/place?kw=building+hardware&city=delhi&select=Area,LocationName,CompanyName,CompanyDescription,Product.cat3,Product.id,LatLong,City,CustomerType&lat=28.6479&lon=77.2342&fromkm=0&tokm=20"
                      |}
                    """.stripMargin
                  }
                }
              } ~
                path("search" / Segment / Segment) { (index, esType) =>
                  parameters(
                    'kw.as[String] ? "", 'city ? "", 'area ? "", 'pin ? "",
                    'category ? "", 'id ? "",
                    'size.as[Int] ? 20, 'offset.as[Int] ? 0,
                    'lat.as[Double] ? 0.0d, 'lon.as[Double] ? 0.0d, 'fromkm.as[Double] ? 0d, 'tokm.as[Double] ? 20.0d,
                    'source.as[Boolean] ? false, 'explain.as
                      [Boolean] ? false,
                    'sort ? "CustomerType.DESC,_score",
                    'select ? "_id",
                    'agg.as[Boolean] ? true, 'aggbuckets.as[Int] ? 10,
                    'maxdocspershard.as[Int] ? 100000,
                    'timeoutms.as[Long] ?
                      2000l, 'searchtype.as[String] ? "query_then_fetch") {
                    (kw, city, area, pin, category, id,
                     size, offset,
                     lat, lon, fromkm,
                     tokm,
                     source,
                     explain,
                     sort,
                     select,
                     agg, aggbuckets,
                     maxdocspershard,
                     timeoutms, searchType) =>
                      val fuzzyprefix = 3
                      val fuzzysim = 0.85f
                      val start =
                        System.
                          currentTimeMillis
                      respondWithMediaType(`application/json`) {
                        complete {
                          implicit val execctx = serverContext.userExecutionContext
                          future {
                            var
                            query: BaseQueryBuilder = null
                            if (kw != null && kw.trim != "") {
                              val kwquery = boolQuery()
                              val w = kw.split( """\s+""")
                              val searchFields = Map("LocationName" -> 80, "CompanyName" -> 80, "CompanyKeywords" -> 80,
                                "Product.l3category" -> 80,
                                "Product.categorykeywords" -> 80, "Product.l2category" -> 1)

                              searchFields.foreach {
                                field: (String, Int) => {
                                  val fieldQuery = spanOrQuery().boost(field._2)
                                  val terms = w
                                    .map(fuzzyQuery(field._1, _).prefixLength(fuzzyprefix).fuzziness(Fuzziness.fromSimilarity(fuzzysim)))
                                    .map(spanMultiTermQueryBuilder)
                                  (1 to Math.min(terms.length, 4)).foreach { len =>
                                    terms.sliding(len).foreach { shingle =>
                                      val nearQuery = spanNearQuery.slop(shingle.length - 1).inOrder(false).boost(field._2 * shingle.length)
                                      shingle.foreach(nearQuery.clause)
                                      fieldQuery.clause(nearQuery)
                                    }
                                  }
                                  kwquery.should(nestIfNeeded(field._1, fieldQuery))
                                }
                              }

                              val condFields = Map(
                                "Product.stringattribute.question" -> Map(
                                  "brands" -> Map("Product.stringattribute.answer" -> 80),
                                  "product" -> Map("Product.stringattribute.answer" -> 20),
                                  "services" -> Map("Product.stringattribute.answer" -> 20),
                                  "features" -> Map("Product.stringattribute.answer" -> 1),
                                  "facilities" -> Map("Product.stringattribute.answer" -> 2),
                                  "material" -> Map("Product.stringattribute.answer" -> 2),
                                  "condition" -> Map("Product.stringattribute.answer" -> 4)
                                )
                              )
                              condFields.foreach {
                                field: (String, Map[String, Map[String, Int]]) => {
                                  val fieldQuery = boolQuery()

                                  field._2.foreach {
                                    v: (String, Map[String, Int]) => {
                                      fieldQuery.must(nestIfNeeded(field._1, termQuery(field._1, v._1)))

                                      val mQuery = boolQuery()
                                      v._2.foreach {
                                        subField: (String, Int) => {
                                          val subQuery = spanOrQuery().boost(subField._2)
                                          val terms = w
                                            .map(fuzzyQuery(subField._1, _).prefixLength(fuzzyprefix).fuzziness(Fuzziness.fromSimilarity(fuzzysim)))
                                            .map(spanMultiTermQueryBuilder)
                                          (1 to Math.min(terms.length, 4)).foreach { len =>
                                            terms.sliding(len).foreach { shingle =>
                                              val nearQuery = spanNearQuery.slop(shingle.length - 1).inOrder(false).boost(subField._2 * shingle.length)
                                              shingle.foreach(nearQuery.clause)
                                              subQuery.clause(nearQuery)
                                            }
                                          }
                                          mQuery.should(nestIfNeeded(subField._1, subQuery))
                                        }
                                      }
                                      fieldQuery.must(mQuery)
                                    }
                                  }
                                  kwquery.should(fieldQuery)
                                }
                              }

                              val exactFields = Map("Product.l3categoryexact" -> 80, "Product.categorykeywordsexact" -> 10)
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
                            if (id != "")
                              query = filteredQuery(query, idsFilter(esType).addIds(id.split( ""","""): _*))
                            if (city != "")
                              query = filteredQuery(query, termsFilter("City", city.split( """,""").map(_.trim.toLowerCase): _*).cache(true))
                            if (area != "") {
                              query = filteredQuery(query, boolFilter
                                .should(termsFilter("AreaExact", area.split( """,""").map(_.trim.toLowerCase): _*))
                                .should(termsFilter("AreaSynonymsExact", area.split( """,""").map(_.trim.toLowerCase): _*)).cache(true)
                              )
                            }
                            if (pin != "")
                              query = filteredQuery(query, termsFilter("PinCode", pin.split( """,""").map(_.trim): _*).cache(true))
                            if (category != "") {
                              query = filteredQuery(query, nestedFilter("Product", termsFilter("Product.l3categoryexact", category.split( """#""").map(_.trim.toLowerCase): _*)).cache(true))
                            }
                            if (lat != 0.0d || lon != 0.0d)
                              query = filteredQuery(query,
                                geoDistanceRangeFilter("LatLong")
                                  .point(lat, lon)
                                  .from(fromkm + "km")
                                  .to(tokm + "km")
                                  .optimizeBbox("indexed")
                                  .geoDistance(GeoDistance.SLOPPY_ARC).cache(true))

                            val search = esClient.prepareSearch(index.split(","): _*)
                              .setTypes(esType.split(","): _*)
                              .setSearchType(SearchType.fromString(searchType))
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
                              if (city == "")
                                search.addAggregation(terms("city").field("CityAggr").size(aggbuckets))

                              search.addAggregation(terms("pincodes").field("PinCode").size(aggbuckets))
                              search.addAggregation(terms("area").field("AreaAggr").size(aggbuckets))
                              search.addAggregation(nested("products").path("Product")
                                .subAggregation(terms("categories").field("Product.cat3aggr").size(aggbuckets))
                                .subAggregation(terms("catkw").field("Product.cat3aggr").size(aggbuckets)
                                .subAggregation(terms("kw").field("Product.cat3kwexact").size(aggbuckets))
                                )
                                .subAggregation(nested("attributes").path("Product.stringattribute")
                                .subAggregation(terms("questions").field("Product.stringattribute.qaggr").size(aggbuckets)
                                .subAggregation(terms("answers").field("Product.stringattribute.aaggr").size(aggbuckets))
                                )
                                )
                              )
                              if (lat != 0.0d || lon != 0.0d)
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

                            debug("query [" + pretty(render(parse(search.toString))) + "]")

                            val result = search.execute().actionGet()
                            val cleanKW = kw.trim.toLowerCase
                            val matchedCat = result.getAggregations.get("products").asInstanceOf[Nested].getAggregations.get("catkw").asInstanceOf[Terms].getBuckets
                              .find(b => b.getKey.trim.toLowerCase == cleanKW || (b.getAggregations.get("kw").asInstanceOf[Terms].getBuckets.exists(_.getKey.trim.toLowerCase == cleanKW)))
                              .fold("/search/" + URLEncoder.encode(cleanKW.replaceAll( """\s+""", "-"), "UTF-8"))(k => "/" + URLEncoder.encode(k.getKey.replaceAll( """\s+""", "-"), "UTF-8"))

                            val slug = (if (city != "") "/" + URLEncoder.encode(city.trim.toLowerCase.replaceAll( """\s+""", "-"), "UTF-8") else "") +
                              matchedCat +
                              (if (category != "") "/cat/" + URLEncoder.encode(category.trim.toLowerCase.replaceAll( """\s+""", "-"), "UTF-8") else "") +
                              (if (area != "") "/in/" + URLEncoder.encode(area.trim.toLowerCase.replaceAll( """\s+""", "-"), "UTF-8") else "")

                            val timeTaken = System.currentTimeMillis - start
                            info("[" + clip.toString + "]->[" + httpReq.uri + "]=[" + result.getTookInMillis + "/" + timeTaken + " (" + result.getHits.hits.length + "/" + result.getHits.getTotalHits + ")]")
                            "{ \"slug\": \"" + slug + "\", \"hit-count\": " + result.getHits.hits.length + ", \"server-time-ms\": " + timeTaken + ", \"results\": " + result.toString + " }"

                          }
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
                      """{"acknowledged": true}"""
                    }
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
