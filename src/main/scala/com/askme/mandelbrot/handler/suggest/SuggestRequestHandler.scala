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
import org.elasticsearch.search.aggregations.{Aggregations, AbstractAggregationBuilder}
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket
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

  private def superBoost(len: Int) = math.pow(10, math.min(10,len+1)).toFloat

  private def shingleSpan(field: String, boost: Float, w: Array[String], fuzzyprefix: Int, maxShingle: Int, minShingle: Int = 1, sloppy: Boolean = true, fuzzy: Boolean = true) = {
    val fieldQuery1 = boolQuery.minimumShouldMatch("33%")
    val terms: Array[SpanQueryBuilder] = w.map(x=>
      if(x.length > 8 && fuzzy)
        spanMultiTermQueryBuilder(
          fuzzyQuery(field, x).prefixLength(fuzzyprefix).fuzziness(if(x.length > 12) Fuzziness.TWO else Fuzziness.ONE))
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

  private def currQuery(tokenFields: Map[String, Float],
                        w: Array[String], fuzzy: Boolean = false, sloppy: Boolean = false, tokenRelax: Int = 0) = {

    disMaxQuery.addAll(tokenFields.map(field => shingleSpan(field._1, field._2, w, 1, w.length, math.max(w.length-tokenRelax, 1), sloppy, fuzzy)))
  }

  private def shinglePartition(tokenFields: Map[String, Float], w: Array[String],
                               maxShingle: Int, minShingle: Int = 1, fuzzy: Boolean = false, sloppy: Boolean = false,
                               tokenRelax: Int = 0): BoolQueryBuilder = {

    if(w.length>0)
      boolQuery.minimumNumberShouldMatch(1).shouldAll(
        (math.max(1, math.min(minShingle, w.length)) to math.min(maxShingle, w.length)).map(len=>(w.slice(0, len), w.slice(len, w.length))).map { x =>
          if (x._2.length > 0)
            shinglePartition(tokenFields, x._2, maxShingle, minShingle, fuzzy, sloppy, tokenRelax)
              .must(currQuery(tokenFields, x._1, fuzzy, sloppy, tokenRelax))
          else
            currQuery(tokenFields, x._1, fuzzy, sloppy, tokenRelax)
        }
      )
    else
      boolQuery
  }

  private def fuzzyOrTermQuery(field: String, word: String, exactBoost: Float, fuzzyPrefix: Int, fuzzy: Boolean = true) = {
    if(word.length > 6 && fuzzy)
      fuzzyQuery(field, word).prefixLength(fuzzyPrefix)
        .fuzziness(if(word.length > 10) Fuzziness.TWO else Fuzziness.ONE)
        .boost(if(word.length > 10) exactBoost/3f else exactBoost/2f)
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
      if(locFilter.hasClauses)
        locFilter.should(boolQuery.mustNot(existsQuery("targeting.area")))
    }

    if (lat != 0.0d || lon != 0.0d) {
      locFilter
        .should(geoHashCellQuery("targeting.coordinates").point(lat, lon).precision("5km").neighbors(true))
        .should(boolQuery.mustNot(existsQuery("targeting.coordinates")))
    }


    if (city != "") {
      val cityFilter = boolQuery
      city.split( """,""").map(analyze(esClient, index, "targeting.city", _).mkString(" ")).filter(!_.isEmpty).foreach { c =>
        cityFilter.should(termQuery("targeting.city", c))
      }

      if(cityFilter.hasClauses)
        finalFilter.must(cityFilter)
    }

    if (locFilter.hasClauses) {
      finalFilter.must(locFilter)
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

    val sort = if(lat != 0.0d || lon !=0.0d) "_score,_distance,_count" else "_score,_count"
    val sorters = getSort(sort, lat, lon, areas)

    val w = analyze(esClient, index, "targeting.kw.token", kw)
    val query =
      if(kw.endsWith(" ")) {
        if (w.nonEmpty) {
          disMaxQuery.add(
            shinglePartition(
              Map("targeting.kw.token" -> 1e9f, "targeting.label.token" -> 1e11f),
              w, w.length, 1, fuzzy = false, sloppy = false, tokenRelax = 0
            )
          ).add(
            shinglePartition(
              Map("targeting.kw.token" -> 1f, "targeting.label.token" -> 1e3f),
              w, w.length, 1, fuzzy = true, sloppy = true, tokenRelax = 0
            )
          ).add(
            shinglePartition(
              Map("targeting.kw.token_edge_ngram" -> 1e1f, "targeting.label.token_edge_ngram" -> 1e2f),
              w, w.length, 1, fuzzy = false, sloppy = true, tokenRelax = 0
            )
          ).add(
            shinglePartition(
              Map("targeting.kw.shingle_nospace_edge_ngram" -> 1e0f, "targeting.label.shingle_nospace_edge_ngram" -> 1e1f),
              w, w.length, 1, fuzzy = false, sloppy = true, tokenRelax = 0
            )
          )
        }
        else
          matchAllQuery
      } else {
        if (w.nonEmpty) {
          val front = w.take(w.length - 1)
          val last_raw = kw.split(pat).last.toLowerCase.trim

          val q = disMaxQuery.add(
            shinglePartition(
              Map("targeting.kw.token" -> 1e9f, "targeting.label.token" -> 1e11f),
              w, w.length, 1, fuzzy = false, sloppy = false, tokenRelax = 0
            )
          ).add(
            shinglePartition(
              Map("targeting.kw.token" -> 1e3f, "targeting.label.token" -> 1e5f),
              w, w.length, 1, fuzzy = true, sloppy = true, tokenRelax = 0
            )
          ).add(
            shinglePartition(
              Map("targeting.kw.token_edge_ngram" -> 1e3f, "targeting.label.token_edge_ngram" -> 1e5f),
              w, w.length, 1, fuzzy = false, sloppy = true, tokenRelax = 0
            )
          ).add(
            shinglePartition(
              Map("targeting.kw.shingle_nospace_edge_ngram" -> 1e1f, "targeting.label.shingle_nospace_edge_ngram" -> 1e2f),
              w, w.length, 1, fuzzy = false, sloppy = true, tokenRelax = 0
            )
          )

          val q2 = boolQuery
          if (front.nonEmpty) {
            q2.must(
              disMaxQuery.add(
                shinglePartition(
                  Map("targeting.kw.token" -> 1e9f, "targeting.label.token" -> 1e11f),
                  front, front.length, 1, fuzzy = false, sloppy = false, tokenRelax = 0
                )
              ).add(
                shinglePartition(
                  Map("targeting.kw.token" -> 1e3f, "targeting.label.token" -> 1e5f),
                  front, front.length, 1, fuzzy = true, sloppy = true, tokenRelax = 0
                )
              ).add(
                shinglePartition(
                  Map("targeting.kw.token_edge_ngram" -> 1e3f, "targeting.label.token_edge_ngram" -> 1e5f),
                  front, front.length, 1, fuzzy = false, sloppy = true, tokenRelax = 0
                )
              ).add(
                shinglePartition(
                  Map("targeting.kw.shingle_nospace_edge_ngram" -> 1e1f, "targeting.label.shingle_nospace_edge_ngram" -> 1e2f),
                  front, front.length, 1, fuzzy = false, sloppy = true, tokenRelax = 0
                )
              )
            )
          }

          val q3 = disMaxQuery
            .add(fuzzyOrTermQuery("targeting.label.token_edge_ngram", last_raw, if(tag=="search") 1e17f else 1e19f, 1, fuzzy = true))
            .add(fuzzyOrTermQuery("targeting.label.shingle_nospace_edge_ngram", last_raw, if(tag=="search") 1e15f else 1e17f, 1, fuzzy = true))
          if(front.isEmpty)
            q3.add(fuzzyOrTermQuery("targeting.label.keyword_edge_ngram", last_raw, if(tag=="search") 1e19f else 1e21f, 1, fuzzy = true))

          q3.add(fuzzyOrTermQuery("targeting.kw.token_edge_ngram", last_raw, 1e17f, 1, fuzzy = false))
            .add(fuzzyOrTermQuery("targeting.kw.shingle_nospace_edge_ngram", last_raw, 1e15f, 1, fuzzy = false))
          if (front.isEmpty)
            q3.add(fuzzyOrTermQuery("targeting.kw.keyword_edge_ngram", last_raw, 1e19f, 1, fuzzy = false))



          q.add(if (q2.hasClauses) q2.must(q3) else q3)
        } else
          matchAllQuery
      }

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

    val orders: List[Terms.Order] = (
        Some(Terms.Order.aggregation("score", false)) ::
          (if (lat != 0.0d || lon != 0.0d) Some(Terms.Order.aggregation("geo", true)) else None) ::
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
            val res = parse(response.toString)
            val parsed = if(explain) res else JArray((res\"aggregations"\"suggestions"\"buckets").children.drop(offset).flatMap(h=>(h\"topHit"\"hits"\"hits").children.map(f=>f\"_source" ++ f\"highlight")))

            val size = (res\"aggregations"\"suggestions"\"buckets").children.drop(offset).size
            val endTime = System.currentTimeMillis
            val timeTaken = endTime - startTime
            info("[" + response.getTookInMillis + "/" + timeTaken + (if (response.isTimedOut) " timeout" else "") + "] [" + size + "/" + response.getHits.getTotalHits + (if (response.isTerminatedEarly) " termearly (" + Math.min(maxdocspershard, int("max-docs-per-shard")) + ")" else "") + "] [" + clip.toString + "]->[" + httpReq.uri + "]")
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

