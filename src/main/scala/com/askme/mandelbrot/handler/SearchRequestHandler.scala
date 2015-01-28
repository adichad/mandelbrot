package com.askme.mandelbrot.handler

import java.net.URLEncoder

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse, SearchType}
import org.elasticsearch.client.Client
import org.elasticsearch.common.geo.GeoDistance
import org.elasticsearch.common.unit.{DistanceUnit, Fuzziness, TimeValue}
import org.elasticsearch.index.query.BaseQueryBuilder
import org.elasticsearch.index.query.FilterBuilders._
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.aggregations.bucket.nested.Nested
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.sort.{FieldSortBuilder, ScoreSortBuilder, SortOrder}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConversions._


/**
 * Created by adichad on 08/01/15.
 */


object SearchRequestHandler {

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
    if (parts.length > 1)
      nestedQuery(parts(0), q).scoreMode("max")
    else q
  }


  private def shingleSpan(field: String, boost: Float, w: Array[String], fuzzyprefix: Int, fuzzysim: Float, maxShingle: Int) = {
    val fieldQuery1 = boolQuery.minimumShouldMatch("67%")
    val terms = w
      .map(fuzzyQuery(field, _).prefixLength(fuzzyprefix).fuzziness(Fuzziness.ONE))
      .map(spanMultiTermQueryBuilder)

    (1 to Math.min(terms.length, maxShingle)).foreach { len =>
      terms.sliding(len).foreach { shingle =>
        val nearQuery = spanNearQuery.slop(len - 1).inOrder(false).boost(boost * len)
        shingle.foreach(nearQuery.clause)
        fieldQuery1.should(nearQuery)
      }
    }

    val fieldQuery2 = boolQuery
    val termsExact = w.map(spanTermQuery(field, _))
    (1 to Math.min(terms.length, maxShingle)).foreach { len =>
      termsExact.sliding(len).foreach { shingle =>
        val nearQuery = spanNearQuery.slop(len - 1).inOrder(false).boost(boost * 2 * len * len)
        shingle.foreach(nearQuery.clause)
        fieldQuery2.should(nearQuery)
      }
    }
    nestIfNeeded(field, disMaxQuery.add(fieldQuery1).add(fieldQuery2))
  }

  private def strongMatch(fields: Set[String],
                          condFields: Map[String, Map[String, Set[String]]],
                          w: Array[String], fuzzyprefix: Int, fuzzysim: Float) = {

    val allQuery = boolQuery.minimumNumberShouldMatch(math.ceil(w.length.toFloat * 2f / 3f).toInt).boost(16384f)
    w.foreach {
      word => {
        val wordQuery = boolQuery
        fields.foreach {
          field =>
            wordQuery.should(nestIfNeeded(field, fuzzyQuery(field, word).prefixLength(fuzzyprefix).fuzziness(Fuzziness.ONE)))
            wordQuery.should(nestIfNeeded(field, termQuery(field, word).boost(16384f)))
        }
        condFields.foreach {
          cond: (String, Map[String, Set[String]]) => {
            cond._2.foreach {
              valField: (String, Set[String]) => {
                val perQuestionQuery = boolQuery
                perQuestionQuery.must(termQuery(cond._1, valField._1))
                val answerQuery = boolQuery
                valField._2.foreach {
                  subField: String =>
                    answerQuery.should(fuzzyQuery(subField, word).prefixLength(fuzzyprefix).fuzziness(Fuzziness.ONE))
                    answerQuery.should(termQuery(subField, word).boost(16384f))
                }
                perQuestionQuery.must(answerQuery)
                wordQuery.should(nestIfNeeded(cond._1, perQuestionQuery))
              }
            }
          }
        }
        allQuery.should(wordQuery)
      }
    }

    allQuery.must(boolQuery
      .should(termQuery("CustomerType", "275"))
      .should(termQuery("CustomerType", "300"))
      .should(termQuery("CustomerType", "350"))
    )
  }

  private def matchAnalyzed(esClient: Client, index: String, field: String, text: String, keywords: Array[String]): Boolean = {
    analyze(esClient, index, field, text).deep == keywords.deep
  }

  private def analyze(esClient: Client, index: String, field: String, text: String): Array[String] =
    (new AnalyzeRequestBuilder(esClient.admin.indices, index, text)).setField(field).get().getTokens.map(_.getTerm).toArray


  private val searchFields = Map("LocationName" -> 512f,
    "Product.l3category" -> 2048f, "BusinessType"->1024f,
    "Product.categorykeywords" -> 2048f, "Product.l2category" -> 8f)

  private val condFields = Map(
    "Product.stringattribute.question" -> Map(
      "brands" -> Map("Product.stringattribute.answer" -> 1024f),
      "menu" -> Map("Product.stringattribute.answer" -> 512f),
      "product" -> Map("Product.stringattribute.answer" -> 256f),
      "services" -> Map("Product.stringattribute.answer" -> 16f),
      "features" -> Map("Product.stringattribute.answer" -> 2f),
      "facilities" -> Map("Product.stringattribute.answer" -> 4f),
      "material" -> Map("Product.stringattribute.answer" -> 4f),
      "condition" -> Map("Product.stringattribute.answer" -> 8f)
    )
  )

  private val condFieldSet = condFields.mapValues(v => v.mapValues(sv => sv.keySet))

  private val exactFields = Map("Product.l3category" -> 4096f, "Product.categorykeywords" -> 4096f, "LocationName" -> 1048576f)

  private val emptyStringArray = new Array[String](0)

}

class SearchRequestHandler(val config: Config, serverContext: SearchContext) extends Actor with Configurable with Logging {

  import com.askme.mandelbrot.handler.SearchRequestHandler._
  private val esClient: Client = serverContext.esClient
  private var w = emptyStringArray

  private def buildSearch(searchParams: SearchParams) = {
    import searchParams.filters._
    import searchParams.geo._
    import searchParams.idx._
    import searchParams.limits._
    import searchParams.page._
    import searchParams.text._
    import searchParams.view._


    var query: BaseQueryBuilder = null

    if (kw != null && kw.trim != "") {
      w = analyze(esClient, index, "CompanyName", kw)
      debug("analyzed keywords: " + w.toList)
      if (w.length > 0) {
        val kwquery = disMaxQuery

        searchFields.foreach {
          field: (String, Float) => {
            kwquery.add(shingleSpan(field._1, field._2, w, fuzzyprefix, fuzzysim, 4))
          }
        }

        condFields.foreach {
          field: (String, Map[String, Map[String, Float]]) => {
            val conditionalQuery = disMaxQuery
            field._2.foreach {
              v: (String, Map[String, Float]) => {
                val perQuestionQuery = boolQuery
                perQuestionQuery.must(nestIfNeeded(field._1, termQuery(field._1, v._1)))
                val answerQuery = disMaxQuery
                v._2.foreach {
                  subField: (String, Float) =>
                    answerQuery.add(shingleSpan(subField._1, subField._2, w, fuzzyprefix, fuzzysim, 4))
                }
                perQuestionQuery.must(answerQuery)
                conditionalQuery.add(perQuestionQuery)
              }
            }
            kwquery.add(conditionalQuery)
          }
        }

        exactFields.foreach {
          field: (String, Float) => {
            val termsExact = w.map(spanTermQuery(field._1, _).boost(field._2))
            val nearQuery = spanNearQuery.slop(0).inOrder(true)
            termsExact.foreach(nearQuery.clause)
            kwquery.add(boolQuery.should(nestIfNeeded(field._1, spanFirstQuery(nearQuery, termsExact.length + 1))).boost(field._2 * 2 * w.length * w.length * (searchFields.size + condFields.values.size + 1)))
          }
        }
        kwquery.add(strongMatch(searchFields.keySet, condFieldSet, w, fuzzyprefix, fuzzysim))

        query = kwquery
      }
    }

    // filters
    if (id != "")
      query = filteredQuery(query, idsFilter(esType).addIds(id.split( ""","""): _*))
    if (city != "")
      query = filteredQuery(query, termsFilter("City", city.split( """,""").map(_.trim.toLowerCase): _*).cache(true))
    val locFilter = boolFilter
    if (area != "") {
      val areas = area.split( """,""")
      areas.map(a => queryFilter(matchPhraseQuery("Area", a).slop(1)).cache(true)).foreach(locFilter.should)
      areas.map(a => queryFilter(matchPhraseQuery("AreaSynonyms", a)).cache(true)).foreach(locFilter.should)
      areas.map(a => termsFilter("AreaSlug", a)).foreach(locFilter.should)
      areas.map(a => termsFilter("AreaSlug", a+"-")).foreach(locFilter.should)

    }

    if (lat != 0.0d || lon != 0.0d)
      locFilter.should(
        geoDistanceRangeFilter("LatLong")
          .point(lat, lon)
          .from((if (area == "") fromkm else 0.0d) + "km")
          .to((if (area == "") tokm else 10.0d) + "km")
          .optimizeBbox("indexed")
          .geoDistance(GeoDistance.SLOPPY_ARC).cache(true))

    if (locFilter.hasClauses)
      query = filteredQuery(query, locFilter)
    if (pin != "")
      query = filteredQuery(query, termsFilter("PinCode", pin.split( """,""").map(_.trim): _*).cache(true))
    if (category != "") {
      query = filteredQuery(query, nestedFilter("Product", termsFilter("Product.l3categoryslug", category.split( """#""") ++ (category.split("""#""").map(_+"-")): _*)).cache(true))
    }


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
      search.addAggregation(
        terms("categories").field("Product.l3categoryexact").size(aggbuckets).order(Terms.Order.aggregation("sum_score", false))
          .subAggregation(sum("sum_score").script("_score"))
      )

      /*
      search.addAggregation(
        terms("apl")
          .script("if([100, 275, 300, 350].grep(doc['custtype'].value)) return 1; else return 0;")
          .subAggregation(
            topHits("best").addSort(new ScoreSortBuilder().order(SortOrder.DESC)).setSize(size+offset)
          )
      )
*/
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


    if (slugFlag) {
      search.addAggregation(nested("products").path("Product")
        .subAggregation(terms("catkw").field("Product.l3categoryexact").size(aggbuckets).order(Terms.Order.aggregation("sum_score", false))
        .subAggregation(terms("kw").field("Product.categorykeywordsexact").size(aggbuckets))
        .subAggregation(sum("sum_score").script("_score"))
        )
      )
      search.addAggregation(terms("areasyns").field("AreaAggr").size(aggbuckets)
        .subAggregation(terms("syns").field("AreaSynonymsExact").size(aggbuckets))
      )
    }
    search
  }

  case class WrappedResponse(searchParams: SearchParams, result: SearchResponse)

  override def receive = {
    case searchParams: SearchParams =>
      val search = buildSearch(searchParams)
      val me = context.self

      search.execute(new ActionListener[SearchResponse] {
        override def onResponse(response: SearchResponse): Unit = {
          me ! WrappedResponse(searchParams, response)
        }
        override def onFailure(e: Throwable): Unit = throw e
      })
      debug("query [" + pretty(render(parse(search.toString))) + "]")
    case response: WrappedResponse =>
      import response.result
      import response.searchParams.filters._
      import response.searchParams.geo._
      import response.searchParams.idx._
      import response.searchParams.req._
      import response.searchParams.view._
      import response.searchParams.startTime

      val cleanKW = w.mkString(" ")
      val areaWords = analyze(esClient, index, "Area", area)

      var slug = ""
      if (slugFlag) {
        val matchedCat = result.getAggregations.get("products").asInstanceOf[Nested].getAggregations.get("catkw").asInstanceOf[Terms].getBuckets
          .find(b => matchAnalyzed(esClient, index, "Product.l3category", b.getKey, w) || (b.getAggregations.get("kw").asInstanceOf[Terms].getBuckets.exists(c => matchAnalyzed(esClient, index, "Product.categorykeywords", c.getKey, w))))
          .fold("/search/" + URLEncoder.encode(cleanKW.replaceAll( """\s+""", "-"), "UTF-8"))(
            k => "/" + URLEncoder.encode(k.getKey.replaceAll("-", " ").replaceAll("&", " ").replaceAll( """\s+""", "-"), "UTF-8"))

        val matchedArea = result.getAggregations.get("areasyns").asInstanceOf[Terms].getBuckets
          .find(b => matchAnalyzed(esClient, index, "Area", b.getKey, areaWords) || (b.getAggregations.get("syns").asInstanceOf[Terms].getBuckets.exists(c => matchAnalyzed(esClient, index, "AreaSynonyms", c.getKey, areaWords))))
          .fold("/in/" + URLEncoder.encode(areaWords.mkString("-"), "UTF-8"))(
            k => "/in/" + URLEncoder.encode(k.getKey.replaceAll("-", " ").replaceAll("&", " ").replaceAll( """\s+""", "-"), "UTF-8"))

        slug = (if (city != "") "/" + URLEncoder.encode(city.trim.toLowerCase.replaceAll( """\s+""", "-"), "UTF-8") else "") +
          matchedCat +
          (if (category != "") "/cat/" + URLEncoder.encode(category.trim.toLowerCase.replaceAll("-", " ").replaceAll("&", " ").replaceAll( """\s+""", "-"), "UTF-8") else "") +
          (if (area != "") matchedArea else "")
      }
      val timeTaken = System.currentTimeMillis - startTime
      info("[" + clip.toString + "]->[" + httpReq.uri + "]=[" + result.getTookInMillis + "/" + timeTaken + " (" + result.getHits.hits.length + "/" + result.getHits.getTotalHits + ")]")

      context.parent ! SearchResult(slug, result.getHits.hits.length, timeTaken, parse(result.toString))

  }

}

