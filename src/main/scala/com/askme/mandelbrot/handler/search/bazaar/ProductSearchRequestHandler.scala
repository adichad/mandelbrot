package com.askme.mandelbrot.handler.search.bazaar

import java.net.URLEncoder
import java.util

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.EmptyResponse
import com.askme.mandelbrot.handler.message.ErrorResponse
import com.askme.mandelbrot.handler.search.bazaar.message.ProductSearchParams
import com.askme.mandelbrot.handler.search.message.SearchResult
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.analyze.{AnalyzeAction, AnalyzeRequestBuilder}
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse, SearchType}
import org.elasticsearch.client.Client
import org.elasticsearch.common.ParseFieldMatcher
import org.elasticsearch.common.unit.{Fuzziness, TimeValue}
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.index.query._
import org.elasticsearch.script.ScriptService.ScriptType
import org.elasticsearch.script._
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder
import org.elasticsearch.search.sort.SortBuilders._
import org.elasticsearch.search.sort._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConversions._


/**
 * Created by adichad on 08/01/15.
 */
object ProductSearchRequestHandler extends Logging {

  val pat = """(?U)[^\p{alnum}]+"""
  val idregex = """[uU]\d+[lL]\d+""".r

  private val randomParams = new util.HashMap[String, AnyRef]
  randomParams.put("buckets", int2Integer(5))

  private def getSort(sort: String, w: Array[String]) = {
    val parts = for (x <- sort.split(",")) yield x.trim
    parts.map {
      case "popularity" => scoreSort.order(SortOrder.DESC)
      case x =>
        val pair = x.split( """\.""", 2)
        if (pair.size == 2)
          new FieldSortBuilder(pair(0)).order(SortOrder.valueOf(pair(1)))
        else
          new FieldSortBuilder(pair(0)).order(SortOrder.DESC)
    }
  }


  private def nestIfNeeded(fieldName: String, q: QueryBuilder): QueryBuilder = {
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

  private def shingleSpan(field: String, boost: Float, w: Array[String], fuzzyprefix: Int, maxShingle: Int, minShingle: Int = 1, sloppy: Boolean = true, fuzzy: Boolean = true) = {
    val fieldQuery1 = boolQuery.minimumShouldMatch("33%")
    val terms: Array[SpanQueryBuilder] = w.map(x=>
      if(x.length > 4 && fuzzy)
        spanMultiTermQueryBuilder(
          fuzzyQuery(field, x).prefixLength(fuzzyprefix).fuzziness(if(x.length > 8) Fuzziness.TWO else Fuzziness.ONE))
      else
        spanTermQuery(field, x)
    )

    (minShingle to Math.min(terms.length, maxShingle)).foreach { len =>
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
      }
    }
    nestIfNeeded(field, fieldQuery1)
  }

  private def shingleFull(field: String, boost: Float, w: Array[String], fuzzyprefix: Int, maxShingle: Int, minShingle: Int = 1, fuzzy: Boolean = true) = {
    val fieldQuery = boolQuery.minimumShouldMatch("33%")
    (minShingle to math.min(maxShingle, w.length)).foreach { len =>
      val lboost = boost * superBoost(len)
      w.sliding(len).foreach { shingle =>
        fieldQuery.should(fuzzyOrTermQuery(field, shingle.mkString(" "), lboost, fuzzyprefix, fuzzy))
      }
    }
    nestIfNeeded(field, fieldQuery)
  }

  val forceFuzzy = Set()
  val forceSpan = Map("name.exact"->"name", "description.exact"->"description",
    "category.description.exact"->"category.description", "category.name.exact"->"category.name")

  private def currQuery(tokenFields: Map[String, Float],
                recomFields: Map[String, Float],
                w: Array[String], fuzzy: Boolean = false, sloppy: Boolean = false, span: Boolean = false, tokenRelax: Int = 0) = {

    if(span)
      disMaxQuery.addAll(tokenFields.map(field => shingleSpan(field._1, field._2, w, 1, w.length, math.max(w.length-tokenRelax, 1), sloppy, if(forceFuzzy.contains(field._1)) true else fuzzy)))
    else {
      disMaxQuery.addAll(recomFields.map(field =>
        if(forceSpan.isDefinedAt(field._1))
          shingleSpan(forceSpan.getOrDefault(field._1, ""), field._2, w, 1, w.length, math.max(w.length-tokenRelax, 1), sloppy, if(forceFuzzy.contains(field._1)) true else fuzzy)
        else
          shingleFull(field._1, field._2, w, 1, w.length, math.max(w.length - tokenRelax, 1), if(forceFuzzy.contains(field._1)) true else fuzzy))
      )
    }
  }

  private def shinglePartition(tokenFields: Map[String, Float], recomFields: Map[String, Float], w: Array[String],
                               maxShingle: Int, minShingle: Int = 1, fuzzy: Boolean = false, sloppy: Boolean = false,
                               span: Boolean = false, tokenRelax: Int = 0): BoolQueryBuilder = {

    if(w.length>0)
      boolQuery.minimumNumberShouldMatch(1).shouldAll(
        (math.max(1, math.min(minShingle, w.length)) to math.min(maxShingle, w.length)).map(len=>(w.slice(0, len), w.slice(len, w.length))).map { x =>
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
      if(word.length > 4 && fuzzy)
        fuzzyQuery(field, word).prefixLength(fuzzyPrefix)
          .fuzziness(if(word.length > 8) Fuzziness.TWO else Fuzziness.ONE)
          .boost(if(word.length > 8) exactBoost/3f else exactBoost/2f)
      else
        termQuery(field, word).boost(exactBoost)

  }

  private def matchAnalyzed(esClient: Client, index: String, field: String, text: String, keywords: Array[String]): Boolean = {
    if(keywords.isEmpty) false else analyze(esClient, index, field, text).deep == keywords.deep
  }

  private def weakMatchAnalyzed(esClient: Client, index: String, field: String, text: String, keywords: Array[String]): Boolean = {
    val textWords = analyze(esClient, index, field, text)
    if(keywords.length > textWords.length) false else textWords.zip(keywords).forall(x=>x._1==x._2)
  }

  private def analyze(esClient: Client, index: String, field: String, text: String): Array[String] =
    new AnalyzeRequestBuilder(esClient.admin.indices, AnalyzeAction.INSTANCE, index, text).setField(field).get().getTokens.map(_.getTerm).toArray


  private val searchFields2 = Map(
    "name" -> 1000000000f,
    "description" -> 10000000f,
    "category.name" -> 1000000000f,
    "category.description" -> 100f)

  private val fullFields2 = Map(
    "name.exact"->100000000000f,
    "description.exact" -> 10000000f,
    "category.name.exact"->100000000000f,
    "category.description.exact" -> 100f)


  private val emptyStringArray = new Array[String](0)

  private def superBoost(len: Int) = math.pow(10, math.min(10,len+1)).toFloat

  private case class WrappedResponse(searchParams: ProductSearchParams, result: SearchResponse, relaxLevel: Int)
  private case class ReSearch(searchParams: ProductSearchParams, filter: BoolQueryBuilder, search: SearchRequestBuilder, relaxLevel: Int, response: SearchResponse)

  private def queryBuilder(tokenFields: Map[String, Float], recomFields: Map[String, Float], fuzzy: Boolean = false, sloppy: Boolean = false, span: Boolean = false, minShingle: Int = 1, tokenRelax: Int = 0)
                          (w: Array[String], maxShingle: Int) = {
    shinglePartition(tokenFields, recomFields, w, maxShingle, minShingle, fuzzy, sloppy, span, tokenRelax)
  }

  private val qDefs: Seq[((Array[String], Int)=>QueryBuilder, Int)] = Seq(

    (queryBuilder(searchFields2, fullFields2, fuzzy = false, sloppy = true, span = false, 1, 0), 1), //1
    // full-shingle exact full matches

    (queryBuilder(searchFields2, fullFields2, fuzzy = true, sloppy = true, span = false, 1, 0), 1), //3
    // full-shingle fuzzy full matches

    //(queryBuilder(searchFields2, fullFields2, fuzzy = false, sloppy = true, span = true, 1, 0), 1), //5
    // full-shingle exact sloppy-span matches

    //(queryBuilder(searchFields2, fullFields2, fuzzy = true, sloppy = false, span = false, 1, 1), 1), //6
    // relaxed-shingle fuzzy full matches

    (queryBuilder(searchFields2, fullFields2, fuzzy = true, sloppy = true, span = true, 2, 1), 1)//7
    // relaxed-shingle fuzzy sloppy-span matches

  )

}

class ProductSearchRequestHandler(val config: Config, serverContext: SearchContext) extends Actor with Configurable with Logging {
  import ProductSearchRequestHandler._
  private val esClient: Client = serverContext.esClient
  private var w = emptyStringArray
  private var kwids: Array[String] = emptyStringArray

  private def buildFilter(searchParams: ProductSearchParams): BoolQueryBuilder = {
    import searchParams.filters._
    import searchParams.idx._


    // filters
    val finalFilter = boolQuery()
    finalFilter.must(termQuery("status", 1))
    if (product_id != 0) {
      finalFilter.must(termQuery("product_id", product_id))
    }
    if (base_id != 0) {
      finalFilter.must(termQuery("base_product_id", base_id))
    }

    val subscriptionFilter = boolQuery().must(termQuery("subscriptions.status", 1))
    if(grouped_id != 0) {
      subscriptionFilter.must(
        termQuery("subscriptions.product_id", grouped_id)
      )
    }
    if(subscribed_id != 0) {
      subscriptionFilter.must(
        termQuery("subscriptions.subscribed_product_id", subscribed_id)
      )
    }

    if (city != "") {
      val cityFilter = boolQuery
      (city+",All City").split( """,""").map(analyze(esClient, index, "subscriptions.ndd_city.exact", _).mkString(" ")).filter(!_.isEmpty).foreach { c =>
        cityFilter.should(termQuery("subscriptions.ndd_city.exact", c))
      }

      if(cityFilter.hasClauses)
        subscriptionFilter.must(cityFilter)
    }

    if(store!="") {
      val storeName = store.toLowerCase.trim
      finalFilter.must(termQuery("stores.name", storeName))
      if(storeName == "flash")
        subscriptionFilter
          .must(rangeQuery("subscriptions.flash_sale_start_date").lte("now"))
          .must(rangeQuery("subscriptions.flash_end_start_date").gte("now"))
    }

    if(subscriptionFilter.hasClauses)
      finalFilter.must(nestedQuery("subscriptions", subscriptionFilter))


    if (category != "") {
      val b = boolQuery
      category.split("""#""").map(analyze(esClient, index, "categories.name.exact", _).mkString(" ")).filter(!_.isEmpty).foreach { cat =>
        b.should(termQuery("categories.name.exact", cat))
      }
      if(b.hasClauses)
        finalFilter.must(b)
    }

    finalFilter
  }

  private def buildSearch(searchParams: ProductSearchParams): SearchRequestBuilder = {
    import searchParams.idx._
    import searchParams.limits._
    import searchParams.page._
    import searchParams.view._
    import searchParams.filters._

    val sorters = getSort(sort, w)

    val search: SearchRequestBuilder = esClient.prepareSearch(index.split(","): _*)
      .setTypes(esType.split(","): _*)
      .setTrackScores(true)
      .setTimeout(TimeValue.timeValueMillis(Math.min(timeoutms, long("timeoutms"))))
      .setTerminateAfter(Math.min(maxdocspershard, int("max-docs-per-shard")))
      .setExplain(explain)
      .setSearchType(SearchType.fromString(searchType, ParseFieldMatcher.STRICT))
      .addSorts(sorters)
      .setFrom(offset).setSize(size)
      .setFetchSource(select.split(""","""), Array[String]())


    if (agg) {
      if (category == ""||category.contains(""","""))
        search.addAggregation(terms("categories").field("categories.name.agg").size(aggbuckets))


      search.addAggregation(
        nested("attributes").path("attributes")
          .subAggregation(terms("attributes").field("attributes.name.agg").size(aggbuckets)
            .subAggregation(terms("vals").field("attributes.value.agg").size(aggbuckets))))
    }

    search
  }


  override def receive = {
      case searchParams: ProductSearchParams =>
        try {
          import searchParams.filters._
          import searchParams.idx._
          import searchParams.req._
          import searchParams.startTime
          import searchParams.text._

          kwids = idregex.findAllIn(kw).toArray.map(_.trim.toUpperCase)
          w = if (kwids.length > 0) emptyStringArray else analyze(esClient, index, "name", kw)
          if (w.length>20) w = emptyStringArray
          w = w.take(8)
          if (w.isEmpty && kwids.isEmpty && category.trim == "" &&
            product_id == 0 && grouped_id == 0 && base_id == 0 && subscribed_id == 0) {
            context.parent ! EmptyResponse("empty search criteria")
          }
          else {
            val query =
              if (w.length > 0) qDefs.head._1(w, w.length)
              else matchAllQuery()
            val leastCount = qDefs.head._2
            val isMatchAll = query.isInstanceOf[MatchAllQueryBuilder]

            // filters
            val finalFilter = buildFilter(searchParams)

            val search = buildSearch(searchParams)

            search.setQuery(boolQuery.must(query).filter(finalFilter))

            val me = context.self

            search.execute(new ActionListener[SearchResponse] {
              override def onResponse(response: SearchResponse): Unit = {
                if (response.getHits.totalHits() >= leastCount || isMatchAll)
                  me ! WrappedResponse(searchParams, response, 0)
                else
                  me ! ReSearch(searchParams, finalFilter, search, 1, response)
              }

              override def onFailure(e: Throwable): Unit = {
                val timeTaken = System.currentTimeMillis() - startTime
                error("[" + timeTaken + "] [q0] [na/na] [" + clip.toString + "]->[" + httpReq.uri + "]->[]")
                context.parent ! ErrorResponse(e.getMessage, e)
              }
            })
          }
        } catch {
          case e: Throwable =>
            context.parent ! ErrorResponse(e.getMessage, e)
        }

      case ReSearch(searchParams, filter, search, relaxLevel, response) =>
        try {
          import searchParams.req._
          import searchParams.startTime

          if (relaxLevel >= qDefs.length)
            context.self ! WrappedResponse(searchParams, response, relaxLevel - 1)
          else {
            val query = qDefs(relaxLevel)._1(w, w.length)
            val leastCount = qDefs(relaxLevel)._2
            val me = context.self

            search.setQuery(boolQuery.must(query).filter(filter)).execute(new ActionListener[SearchResponse] {
              override def onResponse(response: SearchResponse): Unit = {
                if (response.getHits.totalHits() >= leastCount || relaxLevel >= qDefs.length - 1)
                  me ! WrappedResponse(searchParams, response, relaxLevel)
                else
                  me ! ReSearch(searchParams, filter, search, relaxLevel + 1, response)
              }

              override def onFailure(e: Throwable): Unit = {
                val timeTaken = System.currentTimeMillis() - startTime
                error("[" + timeTaken + "] [q" + relaxLevel + "] [na/na] [" + clip.toString + "]->[" + httpReq.uri + "]->[]")
                context.parent ! ErrorResponse(e.getMessage, e)
              }
            })
          }
        } catch {
          case e: Throwable =>
            context.parent ! ErrorResponse(e.getMessage, e)
        }

      case response: WrappedResponse =>
        try {
          import response.searchParams.limits._
          import response.searchParams.req._
          import response.searchParams.startTime
          import response.{relaxLevel, result}

          val parsedResult = parse(result.toString)

          val endTime = System.currentTimeMillis
          val timeTaken = endTime - startTime

          info("[" + result.getTookInMillis + "/" + timeTaken + (if (result.isTimedOut) " timeout" else "") + "] [q" + relaxLevel + "] [" + result.getHits.hits.length + "/" + result.getHits.getTotalHits + (if (result.isTerminatedEarly) " termearly (" + Math.min(maxdocspershard, int("max-docs-per-shard")) + ")" else "") + "] [" + clip.toString + "]->[" + httpReq.uri + "]")
          context.parent ! SearchResult("", result.getHits.hits.length, timeTaken, relaxLevel, parsedResult)
        } catch {
          case e: Throwable =>
            context.parent ! ErrorResponse(e.getMessage, e)
        }

  }

  def urlize(k: String) =
    URLEncoder.encode(k.replaceAll(pat, " ").trim.replaceAll( """\s+""", "-").toLowerCase, "UTF-8")

}

