package com.askme.mandelbrot.handler.suggest

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.message.ErrorResponse
import com.askme.mandelbrot.handler.search.message.SuggestResult
import com.askme.mandelbrot.handler.suggest.message.SuggestParams
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.analyze.{AnalyzeAction, AnalyzeRequestBuilder}
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse, SearchType}
import org.elasticsearch.client.Client
import org.elasticsearch.common.ParseFieldMatcher
import org.elasticsearch.common.geo.GeoDistance
import org.elasticsearch.common.unit.{Fuzziness, TimeValue}
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.index.query._
import org.elasticsearch.script.Script
import org.elasticsearch.script.ScriptService.ScriptType
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder
import org.elasticsearch.search.sort._
import org.elasticsearch.search.sort.SortBuilders._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.util

import scala.collection.JavaConversions._


/**
 * Created by adichad on 08/01/15.
 */


object SuggestRequestHandler extends Logging {

  val pat = """(?U)[^\p{alnum}]+"""
  val idregex = """[uU]\d+[lL]\d+""".r

  private def getSort(sort: String, lat: Double = 0d, lon: Double = 0d, areas: String = "") = {
    val parts = for (x <- sort.split(",")) yield x.trim
    parts.map {
      case "_score" => scriptSort(new Script("docscoreexponent", ScriptType.INLINE, "native", new util.HashMap[String, AnyRef]), "number").order(SortOrder.DESC)
      case "_count" => new FieldSortBuilder("count").order(SortOrder.DESC)
      case "_distance" =>
        val geoParams = new util.HashMap[String, AnyRef]
        geoParams.put("lat", double2Double(lat))
        geoParams.put("lon", double2Double(lon))
        geoParams.put("areas", areas)

        scriptSort(new Script("geobucketsuggest", ScriptType.INLINE, "native", geoParams), "number").order(SortOrder.ASC)
      case x =>
        val pair = x.split( """\.""", 2)
        if (pair.size == 2)
          new FieldSortBuilder(pair(0)).order(SortOrder.valueOf(pair(1)))
        else
          new FieldSortBuilder(pair(0)).order(SortOrder.DESC)
    }
  }


  private[SuggestRequestHandler] def nestIfNeeded(fieldName: String, q: QueryBuilder): QueryBuilder = {
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

  private[SuggestRequestHandler] def shingleSpan(field: String, boost: Float, w: Array[String], fuzzyprefix: Int, maxShingle: Int, minShingle: Int = 1, sloppy: Boolean = true, fuzzy: Boolean = true) = {
    val fieldQuery1 = boolQuery.minimumShouldMatch("33%")
    val terms: Array[SpanQueryBuilder] = w.map(x=>
      if(x.length>3 && fuzzy)
        spanMultiTermQueryBuilder(
          fuzzyQuery(field, x).prefixLength(fuzzyprefix).fuzziness(if(x.length>7) Fuzziness.TWO else Fuzziness.ONE ))
      else
        spanTermQuery(field, x)
    )

    (minShingle to Math.min(terms.length, maxShingle)).foreach { len =>
      //var i = 100000
      val slop = if(sloppy) len/3 else 0
      terms.sliding(len).foreach { shingle =>
        if(shingle.length>1) {
          val nearQuery = spanNearQuery.slop(slop).inOrder(!sloppy).boost(boost * 2 * len) // * math.max(1,i)
          shingle.foreach(nearQuery.clause)
          fieldQuery1.should(nearQuery)
        }
        else {
          fieldQuery1.should(shingle.head)
        }
        //i /= 10
      }
    }
    fieldQuery1
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
    new AnalyzeRequestBuilder(esClient.admin.indices, AnalyzeAction.INSTANCE, index+"_index_incremental", text).setField(field).get().getTokens.map(_.getTerm).toArray

}

class SuggestRequestHandler(val config: Config, serverContext: SearchContext) extends Actor with Configurable with Logging {
  import SuggestRequestHandler._
  private val esClient: Client = serverContext.esClient
  private var areas: String = ""

  private def buildFilter(suggestParams: SuggestParams): BoolQueryBuilder = {
    import suggestParams.target._
    import suggestParams.geo._
    import suggestParams.idx._


    // filters
    val finalFilter = boolQuery.mustNot(termQuery("deleted", 1l))

    if (id != "") {
      finalFilter.must(idsQuery(esType).addIds(id.split( """,""").map(_.trim.toUpperCase): _*))
    }
    if (tag!= "")
      finalFilter.must(termsQuery("targeting.tag", tag.split(",").map(_.trim):_*))

    val locFilter = boolQuery
    if (area != "") {
      val areas: Array[String] = area.split(""",""").map(analyze(esClient, index, "targeting.area", _).mkString(" ")).filter(!_.isEmpty)
      areas.map(fuzzyOrTermQuery("targeting.area", _, 1f, 1, fuzzy = true)).foreach(a => locFilter should a)
      this.areas = areas.map(analyze(esClient, index, "targeting.area", _).mkString(" ")).mkString("#")
    }

    if (lat != 0.0d || lon != 0.0d)
      locFilter.should(
        geoDistanceRangeQuery("targeting.coordinates")
          .point(lat, lon)
          .from((if (area == "") fromkm else 0.0d) + "km")
          .to((if (area == "") tokm else 10.0d) + "km")
          .optimizeBbox("indexed")
          .geoDistance(GeoDistance.SLOPPY_ARC))


    if (city != "") {
      val cityFilter = boolQuery
      city.split( """,""").map(analyze(esClient, index, "targeting.city", _).mkString(" ")).filter(!_.isEmpty).foreach { c =>
        cityFilter.should(termQuery("targeting.city", c))
      }

      if(cityFilter.hasClauses)
        locFilter.must(cityFilter)
    }

    if (locFilter.hasClauses) {
      finalFilter.should(locFilter)
    }

    finalFilter
  }


  private def buildSearch(suggestParams: SuggestParams): SearchRequestBuilder = {
    import suggestParams.geo._
    import suggestParams.idx._
    import suggestParams.limits._
    import suggestParams.page._
    import suggestParams.view._
    import suggestParams.target._

    val sort = if(lat != 0.0d || lon !=0.0d) "_distance,_score,_count" else "_score,_count"
    val sorters = getSort(sort, lat, lon, areas)

    val wordskw = analyze(esClient, index, "targeting.kw.keyword", kw)
    val wordskwng = analyze(esClient, index, "targeting.kw.keyword_ngram", kw)
    val wordsshnspng = analyze(esClient, index, "targeting.kw.shingle_nospace_ngram", kw)


    val query =
      if(wordskw.length>0 && kw.trim.length>1) {
        val query = disMaxQuery()
          .add(shingleSpan("targeting.kw.keyword", if(tag=="search") 1e18f else 1e15f, wordskw, 1, wordskw.length, wordskw.length, false, false)/*.queryName("1")*/)
          .add(shingleSpan("targeting.kw.keyword", 1e5f, wordskw, 1, wordskw.length, wordskw.length, false, true).queryName("1f"))
          .add(shingleSpan("targeting.kw.keyword_edge_ngram", if(tag=="search") 1e17f else 1e14f, wordskw, 1, wordskw.length, wordskw.length, false, false)/*.queryName("2")*/)
          .add(shingleSpan("targeting.kw.keyword_edge_ngram", 1e5f, wordskw, 1, wordskw.length, wordskw.length, false, true).queryName("2f"))
          .add(shingleSpan("targeting.kw.shingle", 1e9f, wordskw, 1, wordskw.length, wordskw.length, false, false)/*.queryName("3")*/)
          .add(shingleSpan("targeting.kw.shingle", 1e4f, wordskw, 1, wordskw.length, wordskw.length, false, true).queryName("3f"))
          .add(shingleSpan("targeting.kw.token", 1e8f, wordskw, 1, wordskw.length, wordskw.length, false, false)/*.queryName("4")*/)
          .add(shingleSpan("targeting.kw.token", 1e3f, wordskw, 1, wordskw.length, wordskw.length, false, false).queryName("4f"))
          .add(shingleSpan("targeting.kw.shingle_nospace", 1e7f, wordskw, 1, wordskw.length, wordskw.length, false, false)/*.queryName("5")*/)
          .add(shingleSpan("targeting.kw.shingle_nospace", 1e3f, wordskw, 1, wordskw.length, wordskw.length, false, true).queryName("5f"))
          .add(shingleSpan("targeting.kw.shingle_edge_ngram", 1e6f, wordskw, 1, wordskw.length, wordskw.length, false, false)/*.queryName("6")*/)
          .add(shingleSpan("targeting.kw.shingle_edge_ngram", 1e3f, wordskw, 1, wordskw.length, wordskw.length, false, true).queryName("6f"))
          .add(shingleSpan("targeting.kw.token_edge_ngram", 1e5f, wordskw, 1, wordskw.length, wordskw.length, false, false)/*.queryName("7")*/)
          .add(shingleSpan("targeting.kw.shingle_nospace_edge_ngram", 1e4f, wordskw, 1, wordskw.length, wordskw.length, false, false)/*.queryName("8")*/)
          .add(shingleSpan("targeting.kw.keyword_ngram", 1e3f, wordskw, 1, wordskw.length, wordskw.length, false, false)/*.queryName("9")*/)
          .add(shingleSpan("targeting.label.keyword", 1e18f, wordskw, 1, wordskw.length, wordskw.length, false, false)/*.queryName("label_1")*/)
          .add(shingleSpan("targeting.label.keyword_edge_ngram", 1e17f, wordskw, 1, wordskw.length, wordskw.length, false, false)/*.queryName("label_2")*/)
          .add(shingleSpan("targeting.label.shingle", 1e12f, wordskw, 1, wordskw.length, wordskw.length, false, false)/*.queryName("label_3")*/)
          .add(shingleSpan("targeting.label.token", 1e11f, wordskw, 1, wordskw.length, wordskw.length, false, false)/*.queryName("label_4")*/)
          .add(shingleSpan("targeting.label.shingle_nospace", 1e10f, wordskw, 1, wordskw.length, wordskw.length, false, false)/*.queryName("label_5")*/)
          .add(shingleSpan("targeting.label.shingle_edge_ngram", 1e9f, wordskw, 1, wordskw.length, wordskw.length, false, false)/*.queryName("label_6")*/)
          .add(shingleSpan("targeting.label.token_edge_ngram", 1e8f, wordskw, 1, wordskw.length, wordskw.length, false, false)/*.queryName("label_7")*/)
          .add(shingleSpan("targeting.label.shingle_nospace_edge_ngram", 1e7f, wordskw, 1, wordskw.length, wordskw.length, false, false)/*.queryName("label_8")*/)
          .add(shingleSpan("targeting.label.keyword_ngram", 1e6f, wordskw, 1, wordskw.length, wordskw.length, false, false)/*.queryName("label_9")*/)

        if(wordskwng.length>0)
          query
            .add(shingleSpan("targeting.kw.keyword_ngram", 1e2f, wordskwng, 1, wordskwng.length, wordskwng.length, true, false)/*.queryName("10")*/)
            .add(shingleSpan("targeting.label.keyword_ngram", 1e5f, wordskwng, 1, wordskwng.length, wordskwng.length, true, false)/*.queryName("label_10")*/)
        if(wordsshnspng.length>0)
          query
            .add(shingleSpan("targeting.kw.shingle_nospace_ngram", 1e1f, wordsshnspng, 1, wordsshnspng.length, wordsshnspng.length, true, false)/*.queryName("11")*/)
            .add(shingleSpan("targeting.label.shingle_nospace_ngram", 1e4f, wordsshnspng, 1, wordsshnspng.length, wordsshnspng.length, true, false)/*.queryName("label_11")*/)

        query
      }
      else
        matchAllQuery




    val search: SearchRequestBuilder = esClient.prepareSearch(index.split(","): _*)
      .setTypes(esType.split(","): _*)
      .setTrackScores(false)
      .setTimeout(TimeValue.timeValueMillis(Math.min(timeoutms, long("timeoutms"))))
      .setTerminateAfter(Math.min(maxdocspershard, int("max-docs-per-shard")))
      .setExplain(false)
      .setSearchType(SearchType.fromString(searchType, ParseFieldMatcher.STRICT))
      .setFrom(0).setSize(0)
      .setFetchSource(false)
      .setQuery(boolQuery.must(query).filter(buildFilter(suggestParams)))

    //val options = new java.util.HashMap[String, AnyRef]
    //options.put("force_source", new java.lang.Boolean(true))

    //val hquery = matchQuery("targeting.kw.highlight", kw)

    val orders: List[Terms.Order] = (
        (if (lat != 0.0d || lon != 0.0d) Some(Terms.Order.aggregation("geo", true)) else None) ::
          Some(Terms.Order.aggregation("score", false)) ::
          Some(Terms.Order.aggregation("count", false)) ::
          Nil
      ).flatten
    val order = if(orders.size==1) orders.head else Terms.Order.compound(orders)
    val masters = terms("suggestions").field("groupby").order(order).size(offset+size)
      .subAggregation(
        topHits("topHit")
          .setFetchSource(select.split(""","""), unselect.split(""","""))
          /*
          .setHighlighterType("postings-highlighter")
          .addHighlightedField("targeting.kw.highlight", 100, 5, 0)
          .setHighlighterQuery(hquery)
          */
          .setSize(1)
          .setExplain(false)
          .setTrackScores(false)
          .addSorts(sorters)
      )

    if(lat != 0.0d || lon !=0.0d) {
      val geoParams = new util.HashMap[String, AnyRef]
      geoParams.put("lat", double2Double(lat))
      geoParams.put("lon", double2Double(lon))
      geoParams.put("areas", areas)

      masters.subAggregation(min("geo").script(new Script("geobucketsuggest", ScriptType.INLINE, "native", geoParams)))
    }

    masters.subAggregation(max("score").script(new Script("docscoreexponent", ScriptType.INLINE, "native", new util.HashMap[String, AnyRef])))
    masters.subAggregation(sum("count").field("count"))
    search.addAggregation(masters)
  }


  override def receive = {

    case suggestParams: SuggestParams =>
      import suggestParams.startTime
      import suggestParams.req._
      import suggestParams.limits._
      import suggestParams.view._
      import suggestParams.page._
      try {

        val search = buildSearch(suggestParams)

        search.execute(new ActionListener[SearchResponse] {
          override def onResponse(response: SearchResponse): Unit = {
            val parsed = if(explain) parse(response.toString) else JArray((parse(response.toString)\"aggregations"\"suggestions"\"buckets").children.drop(offset).flatMap(h=>(h\"topHit"\"hits"\"hits").children.map(f=>f\"_source" ++ f\"highlight")))

            val endTime = System.currentTimeMillis
            val timeTaken = endTime - startTime
            info("[" + response.getTookInMillis + "/" + timeTaken + (if (response.isTimedOut) " timeout" else "") + "] [" + response.getHits.hits.length + "/" + response.getHits.getTotalHits + (if (response.isTerminatedEarly) " termearly (" + Math.min(maxdocspershard, int("max-docs-per-shard")) + ")" else "") + "] [" + clip.toString + "]->[" + httpReq.uri + "]")
            context.parent ! SuggestResult(timeTaken, parsed)
          }

          override def onFailure(e: Throwable): Unit = {
            val timeTaken = System.currentTimeMillis() - startTime
            error("[" + timeTaken + "] ["+e.getMessage+"] [" + clip.toString + "]->[" + httpReq.uri + "]", e)
            context.parent ! ErrorResponse(e.getMessage, e)
          }
        })
      } catch {
        case e: Throwable =>
          val timeTaken = System.currentTimeMillis() - startTime
          error("[" + timeTaken + "] ["+e.getMessage+"] [" + clip.toString + "]->[" + httpReq.uri + "]", e)
          context.parent ! ErrorResponse(e.getMessage, e)
      }

  }

}

