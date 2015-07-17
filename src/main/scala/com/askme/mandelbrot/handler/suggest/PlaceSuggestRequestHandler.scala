package com.askme.mandelbrot.handler.suggest

import java.net.URLEncoder

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.EmptyResponse
import com.askme.mandelbrot.handler.message.ErrorResponse
import com.askme.mandelbrot.handler.search.message.{SuggestResult, SearchParams, SearchResult}
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse, SearchType}
import org.elasticsearch.client.Client
import org.elasticsearch.common.geo.GeoDistance
import org.elasticsearch.common.unit.{Fuzziness, TimeValue}
import org.elasticsearch.index.query.FilterBuilders._
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.index.query._
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.aggregations.bucket.nested.Nested
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder
import org.elasticsearch.search.sort._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConversions._


/**
 * Created by adichad on 08/01/15.
 */


object PlaceSuggestRequestHandler extends Logging {

  val pat = """(?U)[^\p{alnum}]+"""
  val idregex = """[uU]\d+[lL]\d+""".r

  private def getSort(sort: String, lat: Double = 0d, lon: Double = 0d, areaSlugs: String = "", w: Array[String]) = {
    val parts = for (x <- sort.split(",")) yield x.trim
    parts.map(
      _ match {
        case "_name" => SortBuilders.scriptSort("exactnamematch", "number").lang("native").order(SortOrder.ASC).
          param("name", w.mkString(" "))
        case "_score" => new ScoreSortBuilder().order(SortOrder.DESC)
        case "_distance" => SortBuilders.scriptSort("geobucket", "number").lang("native")
          .param("lat", lat).param("lon", lon).param("areaSlugs", areaSlugs)
          .param("coordfield", "targeting.coordinates")
          .param("areafield", "targeting.areadocval").order(SortOrder.ASC)
        case "_ct" => SortBuilders.scriptSort("customertype", "number").lang("native").order(SortOrder.ASC)
        case "_mc" => SortBuilders.scriptSort("mediacountsort", "number").lang("native").order(SortOrder.DESC)
        case x =>
          val pair = x.split( """\.""", 2)
          if (pair.size == 2)
            new FieldSortBuilder(pair(0)).order(SortOrder.valueOf(pair(1)))
          else
            new FieldSortBuilder(pair(0)).order(SortOrder.DESC)
      }
    )
  }


  private[PlaceSuggestRequestHandler] def nestIfNeeded(fieldName: String, q: BaseQueryBuilder): BaseQueryBuilder = {
    /*val parts = fieldName.split("""\.""")
    if (parts.length > 1)
      nestedQuery(parts(0), q).scoreMode("max")
    else
    */
    q
  }



  private implicit class DisMaxQueryPimp(val q: DisMaxQueryBuilder) {
    def addAll(queries: Iterable[QueryBuilder]) = {
      queries.foreach(q.add)
      q
    }
  }

  private implicit class BoolQueryPimp(val q: BoolQueryBuilder) {
    def shouldAll(queries: Iterable[QueryBuilder]) = {
      queries.foreach(q.should)
      q
    }

    def mustAll(queries: Iterable[QueryBuilder]) = {
      queries.foreach(q.must)
      q
    }

    def mustNotAll(queries: Iterable[QueryBuilder]) = {
      queries.foreach(q.mustNot)
      q
    }
  }

  private implicit class SearchPimp(val search: SearchRequestBuilder) {
    def addSorts(sorts: Iterable[SortBuilder]) = {
      sorts.foreach(search.addSort)
      search
    }

    def addAggregations(aggregations: Iterable[AbstractAggregationBuilder]) = {
      aggregations.foreach(search.addAggregation)
      search
    }
  }

  private implicit class AggregationPimp(val agg: TopHitsBuilder) {
    def addSorts(sorts: Iterable[SortBuilder]) = {
      sorts.foreach(agg.addSort)
      agg
    }
  }

  private[PlaceSuggestRequestHandler] def shingleSpan(field: String, boost: Float, w: Array[String], fuzzyprefix: Int, maxShingle: Int, minShingle: Int = 1, sloppy: Boolean = true, fuzzy: Boolean = true) = {
    val fieldQuery1 = boolQuery.minimumShouldMatch("33%")
    val terms: Array[BaseQueryBuilder with SpanQueryBuilder] = w.map(x=>
      if(fuzzy)
        spanMultiTermQueryBuilder(
          fuzzyQuery(field, x).prefixLength(fuzzyprefix).fuzziness(Fuzziness.AUTO))
      else
        spanTermQuery(field, x)
    )

    (minShingle to Math.min(terms.length, maxShingle)).foreach { len =>
      //var i = 100000
      val slop = if(sloppy) len/3 else 0
      terms.sliding(len).foreach { shingle =>
        val nearQuery = spanNearQuery.slop(slop).inOrder(!sloppy).boost(boost * 2 * len) // * math.max(1,i)
        shingle.foreach(nearQuery.clause)
        fieldQuery1.should(nearQuery)
        //i /= 10
      }
    }
    fieldQuery1
  }

  private def shingleFull(field: String, boost: Float, w: Array[String], fuzzyprefix: Int, maxShingle: Int, minShingle: Int = 1, fuzzy: Boolean = true) = {
    val fieldQuery = boolQuery.minimumShouldMatch("33%")
    (minShingle to math.min(maxShingle, w.length)).foreach { len =>
      val lboost = boost * superBoost(len)
      w.sliding(len).foreach { shingle =>
        val phrase = shingle.mkString(" ")
        fieldQuery.should(fuzzyOrTermQuery(field, phrase, lboost, fuzzyprefix, fuzzy))
      }
    }
    nestIfNeeded(field, fieldQuery)
  }

  private def currQuery(tokenFields: Map[String, Float],
                        recomFields: Map[String, Float],
                        w: Array[String], fuzzy: Boolean = false, sloppy: Boolean = false, span: Boolean = false, tokenRelax: Int = 0) = {
    if(span)
      disMaxQuery.addAll(tokenFields.map(field => shingleSpan(field._1, field._2, w, 1, w.length, math.max(w.length-tokenRelax, 1), sloppy, fuzzy)))
    else
      disMaxQuery.addAll(recomFields.map(field => shingleFull(field._1, field._2, w, 1, w.length, math.max(w.length-tokenRelax, 1), fuzzy)))
  }

  private def shinglePartition(tokenFields: Map[String, Float], recomFields: Map[String, Float], w: Array[String],
                               maxShingle: Int, minShingle: Int = 1, fuzzy: Boolean = false, sloppy: Boolean = false,
                               span: Boolean = false, tokenRelax: Int = 0): BoolQueryBuilder = {

    if(w.length>0)
      boolQuery.minimumNumberShouldMatch(1).shouldAll(
        (math.max(1, math.min(minShingle, w.length)) to math.min(maxShingle, w.length)).map(len=>(w.slice(0, len), w.slice(len, w.length))).map { x =>
          //info(x._1.toList.toString+","+x._2.toList.toString)
          if (x._2.length > 0)
            shinglePartition(tokenFields, recomFields, x._2, maxShingle, minShingle, fuzzy, sloppy, span, tokenRelax)
              .must(currQuery(tokenFields, recomFields, x._1, fuzzy, sloppy, span, tokenRelax))
          else
            currQuery(tokenFields, recomFields, x._1, fuzzy, sloppy, span, tokenRelax)
        }
      )
    else
      boolQuery
  }

  private def fuzzyOrTermQuery(field: String, word: String, exactBoost: Float, fuzzyPrefix: Int, fuzzy: Boolean = true) = {
    if(word.length > 8 && fuzzy)
      fuzzyQuery(field, word).prefixLength(fuzzyPrefix)
        .fuzziness(if(word.length > 12) Fuzziness.TWO else Fuzziness.ONE)
        .boost(if(word.length > 12) exactBoost/3f else exactBoost/2f)
    else
      termQuery(field, word).boost(exactBoost)

  }

  private def analyze(esClient: Client, index: String, field: String, text: String): Array[String] =
    new AnalyzeRequestBuilder(esClient.admin.indices, index, text).setField(field).get().getTokens.map(_.getTerm).toArray


  private val emptyStringArray = new Array[String](0)

  private def superBoost(len: Int) = math.pow(10, math.min(10,len+1)).toFloat

}

class PlaceSuggestRequestHandler(val config: Config, serverContext: SearchContext) extends Actor with Configurable with Logging {
  import PlaceSuggestRequestHandler._
  private val esClient: Client = serverContext.esClient
  private var w = emptyStringArray
  private var areaSlugs: String = ""

  private def buildFilter(searchParams: SearchParams): FilterBuilder = {
    import searchParams.filters._
    import searchParams.geo._
    import searchParams.idx._
    import searchParams.limits._
    import searchParams.view._


    // filters
    val finalFilter = andFilter(boolFilter.mustNot(termFilter("deleted", 1l).cache(false))).cache(false)

    if (id != "") {
      finalFilter.add(idsFilter(esType).addIds(id.split( """,""").map(_.trim.toUpperCase): _*))
    }

    val locFilter = boolFilter.cache(false)
    if (area != "") {
      val areas: Array[String] = area.split(""",""").map(analyze(esClient, index, "targeting.area", _).mkString(" ")).filter(!_.isEmpty)
      areas.map(fuzzyOrTermQuery("targeting.area", _, 1f, 1, true)).foreach(a => locFilter should queryFilter(a).cache(false))
      areaSlugs = areas.map(analyze(esClient, index, "targeting.area", _).mkString(" ")).mkString("#")
    }

    if (lat != 0.0d || lon != 0.0d)
      locFilter.should(
        geoDistanceRangeFilter("targeting.coordinates").cache(true)
          .point(lat, lon)
          .from((if (area == "") fromkm else 0.0d) + "km")
          .to((if (area == "") tokm else 10.0d) + "km")
          .optimizeBbox("indexed")
          .geoDistance(GeoDistance.SLOPPY_ARC))


    if (city != "") {
      val cityFilter = boolFilter.cache(false)
      city.split( """,""").map(analyze(esClient, index, "targeting.city", _).mkString(" ")).filter(!_.isEmpty).foreach { c =>
        cityFilter.should(termFilter("targeting.city", c).cache(false))
      }

      if(cityFilter.hasClauses)
        locFilter.must(cityFilter)
    }

    if (locFilter.hasClauses) {
      finalFilter.add(nestedFilter("targeting", locFilter))
    }

    finalFilter
  }

  private def buildQuery(searchParams: SearchParams): QueryBuilder = {
    import searchParams.text._
    import searchParams.idx._
    val query = boolQuery()
    val wordskw = analyze(esClient, index, "targeting.kw.keyword", kw)
    val wordskwsh = analyze(esClient, index, "targeting.kw.shingle", kw)
    val wordskweng = analyze(esClient, index, "targeting.kw.keyword_edge_ngram", kw)
    val wordskwng = analyze(esClient, index, "targeting.kw.keyword_ngram", kw)
    val wordsshnspng = analyze(esClient, index, "targeting.kw.shingle_nospace_ngram", kw)

    nestedQuery("targeting",
      query
        .should(shingleSpan("targeting.kw.keyword", 1e16.toInt, wordskw, 1, wordskw.length, wordskw.length, false, false).queryName("1"))
        .should(shingleSpan("targeting.kw.keyword", 1e15.toInt, wordskw, 1, wordskw.length, wordskw.length, false, true).queryName("2"))
        .should(shingleSpan("targeting.kw.keyword_edge_ngram", 1e15.toInt, wordskw, 1, wordskw.length, wordskw.length, false, false).queryName("3"))
        .should(shingleSpan("targeting.kw.shingle", 1e14.toInt, wordskw, 1, wordskw.length, wordskw.length, false, false).queryName("4"))
        .should(shingleSpan("targeting.kw.shingle", 1e13.toInt, wordskw, 1, wordskw.length, wordskw.length, false, true).queryName("5"))
        .should(shingleSpan("targeting.kw.token", 1e13.toInt, wordskw, 1, wordskw.length, wordskw.length, false, false).queryName("6"))
        .should(shingleSpan("targeting.kw.token", 1e12.toInt, wordskw, 1, wordskw.length, wordskw.length, false, true).queryName("7"))
        .should(shingleSpan("targeting.kw.shingle_nospace", 1e12.toInt, wordskw, 1, wordskw.length, wordskw.length, false, false).queryName("8"))
        .should(shingleSpan("targeting.kw.shingle_nospace", 1e11.toInt, wordskw, 1, wordskw.length, wordskw.length, false, true).queryName("9"))
        .should(shingleSpan("targeting.kw.shingle_edge_ngram", 1e10.toInt, wordskw, 1, wordskw.length, wordskw.length, false, false).queryName("10"))
        .should(shingleSpan("targeting.kw.token_edge_ngram", 1e10.toInt, wordskw, 1, wordskw.length, wordskw.length, false, false).queryName("11"))
        .should(shingleSpan("targeting.kw.shingle_nospace_edge_ngram", 1e9.toInt, wordskw, 1, wordskw.length, wordskw.length, false, false).queryName("12"))
        .should(shingleSpan("targeting.kw.keyword_edge_ngram", 1e8.toInt, wordskw, 1, wordskw.length, wordskw.length, false, true).queryName("13"))
        .should(shingleSpan("targeting.kw.shingle_nospace_edge_ngram", 1e8.toInt, wordskw, 1, wordskw.length, wordskw.length, false, true).queryName("14"))
        .should(shingleSpan("targeting.kw.keyword_ngram", 1e7.toInt, wordskw, 1, wordskw.length, wordskw.length, false, false).queryName("15"))
        .should(shingleSpan("targeting.kw.keyword_ngram", 1e7.toInt, wordskwng, 1, wordskwng.length, wordskwng.length, true, false).queryName("16"))
        .should(shingleSpan("targeting.kw.shingle_nospace_ngram", 1e6.toInt, wordsshnspng, 1, wordsshnspng.length, wordsshnspng.length, true, false).queryName("17"))
    )

  }

  private def buildSearch(searchParams: SearchParams): SearchRequestBuilder = {
    import searchParams.geo._
    import searchParams.idx._
    import searchParams.limits._
    import searchParams.page._
    import searchParams.view._

    val sort = if(lat != 0.0d || lon !=0.0d) "_distance,_score" else "_score"
    val sorters = getSort(sort, lat, lon, areaSlugs, w)

    val search: SearchRequestBuilder = esClient.prepareSearch(index.split(","): _*).setQueryCache(false)
      .setTypes(esType.split(","): _*)
      .setTrackScores(true)
      .setTimeout(TimeValue.timeValueMillis(Math.min(timeoutms, long("timeoutms"))))
      .setTerminateAfter(Math.min(maxdocspershard, int("max-docs-per-shard")))
      .setExplain(explain)
      .setSearchType(SearchType.fromString(searchType))
      .addSorts(sorters)
      .setFrom(0).setSize(0)
      .setFetchSource(select.split(""","""), unselect.split(""","""))


    val orders: List[Terms.Order] = (
        (if (lat != 0.0d || lon != 0.0d) Some(Terms.Order.aggregation("geo", true)) else None) ::
        Some(Terms.Order.aggregation("score", false)) ::
        Nil
      ).flatten
    val order = if(orders.size==1) orders(0) else Terms.Order.compound(orders)
    val masters = terms("suggestions").field("masterid").order(order).size(offset+size)
      .subAggregation(topHits("toplocation").setFetchSource(select.split(""","""), unselect.split(""",""")).setSize(1).setExplain(explain).setTrackScores(true).addSorts(sorters))

    if(lat != 0.0d || lon !=0.0d) {
      masters.subAggregation(min("geo").script("geobucket").lang("native").param("lat", lat).param("lon", lon).param("areaSlugs", areaSlugs).param("coordfield", "targeting.coordinates")
        .param("areafield", "targeting.areadocval"))
    }

    masters.subAggregation(max("score").script("docscore").lang("native"))
    search.addAggregation(masters)
  }


  override def receive = {
    case searchParams: SearchParams =>
      try {
        import searchParams.text._
        import searchParams.idx._
        import searchParams.filters._
        import searchParams.startTime
        import searchParams.req._
        import searchParams.limits._

        w = analyze(esClient, index, "targeting.kw", kw)
        if (w.length>12) w = emptyStringArray
        val query = if (w.length > 0) buildQuery(searchParams) else matchAllQuery()
        // filters
        val finalFilter = buildFilter(searchParams)

        val search = buildSearch(searchParams)

        search.setQuery(filteredQuery(query, finalFilter))

        search.execute(new ActionListener[SearchResponse] {
          override def onResponse(response: SearchResponse): Unit = {
            val parsed = JArray((parse(response.toString)\"aggregations"\"suggestions"\"buckets").children.map(h=>(h\"toplocation"\"hits"\"hits").children.map(f=>f\"_source")).flatten)

            val endTime = System.currentTimeMillis
            val timeTaken = endTime - startTime
            info("[" + response.getTookInMillis + "/" + timeTaken + (if (response.isTimedOut) " timeout" else "") + "] [" + response.getHits.hits.length + "/" + response.getHits.getTotalHits + (if (response.isTerminatedEarly) " termearly (" + Math.min(maxdocspershard, int("max-docs-per-shard")) + ")" else "") + "] [" + clip.toString + "]->[" + httpReq.uri + "]")
            context.parent ! SuggestResult(timeTaken, parsed)
          }

          override def onFailure(e: Throwable): Unit = {
            val timeTaken = System.currentTimeMillis() - startTime
            error("[" + timeTaken + "] [q0] ["+e.getMessage+"] [" + clip.toString + "]->[" + httpReq.uri + "]->[]")
            context.parent ! ErrorResponse(e.getMessage, e)
          }
        })
      } catch {
        case e: Throwable =>
          context.parent ! ErrorResponse(e.getMessage, e)
      }

  }

  def urlize(k: String) =
    URLEncoder.encode(k.replaceAll(pat, " ").trim.replaceAll( """\s+""", "-").toLowerCase, "UTF-8")

}

