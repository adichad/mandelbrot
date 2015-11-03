package com.askme.mandelbrot.handler.search

import java.util

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.EmptyResponse
import com.askme.mandelbrot.handler.message.ErrorResponse
import com.askme.mandelbrot.handler.search.message.{SearchResult, DealSearchParams}
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.analyze.{AnalyzeAction, AnalyzeRequestBuilder}
import org.elasticsearch.action.search.{SearchType, SearchRequestBuilder, SearchResponse}
import org.elasticsearch.client.Client
import org.elasticsearch.common.ParseFieldMatcher
import org.elasticsearch.common.unit.{TimeValue, Fuzziness}
import org.elasticsearch.index.query._
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.script.Script
import org.elasticsearch.script.ScriptService.ScriptType
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.sort._
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConversions._

/**
 * Created by nishantkumar on 7/30/15.
 */

object DealSearchRequestHandler extends Logging {

  private def getSort(sort: String) = {
    val parts = for (x <- sort.split(",")) yield x.trim
    parts.map {
      case "_home" => new FieldSortBuilder("ShowOnHomePage").order(SortOrder.DESC)
      case "_score" => new ScoreSortBuilder().order(SortOrder.DESC)
    }
  }

  private def analyze(esClient: Client, index: String, field: String, text: String): Array[String] =
    new AnalyzeRequestBuilder(esClient.admin.indices, AnalyzeAction.INSTANCE, index, text).setField(field).get().getTokens.map(_.getTerm).toArray

  private implicit class DisMaxQueryPimp(val q: DisMaxQueryBuilder) {
    def addAll(queries: Iterable[QueryBuilder]) = {
      queries.foreach(q.add)
      q
    }
  }

  private[DealSearchRequestHandler] def nestIfNeeded(fieldName: String, q: QueryBuilder): QueryBuilder = {
    /*val parts = fieldName.split("""\.""")
    if (parts.length > 1)
      nestedQuery(parts(0), q).scoreMode("max")
    else
    */
    q
  }

  private def superBoost(len: Int) = math.pow(10, math.min(10,len+1)).toFloat
  private case class WrappedResponse(searchParams: DealSearchParams, result: SearchResponse, relaxLevel: Int)
  private case class ReSearch(searchParams: DealSearchParams, filter: BoolQueryBuilder, search: SearchRequestBuilder, relaxLevel: Int, response: SearchResponse)

  private def fuzzyOrTermQuery(field: String, word: String, exactBoost: Float, fuzzyPrefix: Int, fuzzy: Boolean = true) = {
    if(word.length > 8 && fuzzy)
      fuzzyQuery(field, word).prefixLength(fuzzyPrefix)
        .fuzziness(if(word.length > 12) Fuzziness.TWO else Fuzziness.ONE)
        .boost(if(word.length > 12) exactBoost/3f else exactBoost/2f)
    else
      termQuery(field, word).boost(exactBoost)

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

  private val emptyStringArray = new Array[String](0)
  private val searchFields2 = Map("Title" -> 1000000000f, "Headline" -> 1000000000f,
    "Categories.Name" -> 10000000f,
    "Categories.Synonym" -> 1000f,
    "DealTags"->1000f,
    "Offers.Name" -> 1000f,
    "DescriptionShort" -> 100f,
    "Offers.DescriptionShort" -> 100f,
    "Locations.Area"->10f, "Locations.AreaSynonyms"->10f,
    "Locations.City"->1f, "Locations.CitySynonyms"->1f)

  private val fullFields2 = Map(
    "Title.TitleExact"->100000000000f, "Headline.HeadlineExact"->100000000000f,
    "Categories.Name.NameExact"->10000000000f,
    "Categories.Synonym.SynonymExact"->10000000f,
    "DealTags.DealTagsExact"->10000000f,
    "Offers.Name.NameExact"->1000f,
    "DescriptionShort.DescriptionShortExact"->1000f,
    "Offers.DescriptionShort.DescriptionShortExact" -> 1000f,
    "Locations.Area.AreaExact"->10f, "Locations.AreaSynonyms.AreaSynonymsExact"->10f,
    "Locations.City.CityExact"->1f, "Locations.CitySynonyms.CitySynonymsExacts"->1f)

  private val exactToFieldMap = Map("Title.TitleExact" -> "Title",
    "Headline.HeadlineExact"-> "Headline",
    "Offers.Name.NameExact"-> "Offers.Name",
    "DescriptionShort.DescriptionShortExact" -> "DescriptionShort",
    "Offers.DescriptionShort.DescriptionShortExact" -> "Offers.DescriptionShort")

  private implicit class SearchPimp(val search: SearchRequestBuilder) {
    def addSorts(sorts: Iterable[SortBuilder]) = {
      sorts.foreach(search.addSort)
      search
    }
  }

  private[DealSearchRequestHandler] def shingleSpan(field: String, boost: Float, w: Array[String], fuzzyprefix: Int, maxShingle: Int, minShingle: Int = 1, sloppy: Boolean = true, fuzzy: Boolean = true) = {
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
        val nearQuery = spanNearQuery.slop(slop).inOrder(!sloppy).boost(boost * 2 * len) // * math.max(1,i)
        shingle.foreach(nearQuery.clause)
        fieldQuery1.should(nearQuery)
        //i /= 10
      }
    }
    nestIfNeeded(field, fieldQuery1)
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
      disMaxQuery.addAll(recomFields.map(field =>
        if(exactToFieldMap isDefinedAt field._1)
          shingleSpan(exactToFieldMap(field._1), field._2, w, 1, w.length, math.max(w.length-tokenRelax, 1), sloppy, fuzzy)
        else
          shingleFull(field._1, field._2, w, 1, w.length, math.max(w.length-tokenRelax, 1), fuzzy)))
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

  private def queryBuilder(tokenFields: Map[String, Float], recomFields: Map[String, Float], fuzzy: Boolean = false, sloppy: Boolean = false, span: Boolean = false, minShingle: Int = 1, tokenRelax: Int = 0)
                          (w: Array[String], maxShingle: Int) = {
    shinglePartition(tokenFields, recomFields, w, maxShingle, minShingle, fuzzy, sloppy, span, tokenRelax)
  }

  private val qDefs: Seq[((Array[String], Int)=>QueryBuilder, Int)] = Seq(

    (queryBuilder(searchFields2, fullFields2, fuzzy = false, sloppy = false, span = false, 1, 0), 1), //1
    // full-shingle exact full matches

    (queryBuilder(searchFields2, fullFields2, fuzzy = true, sloppy = false, span = false, 1, 0), 1), //3
    // full-shingle fuzzy full matches

    //(queryBuilder(searchFields2, fullFields2, false, false, true, 2, 0), 1), //1
    // full-shingle exact span matches

    (queryBuilder(searchFields2, fullFields2, fuzzy = true, sloppy = true, span = true, 2, 0), 1), //5
    // full-shingle exact sloppy-span matches

    (queryBuilder(searchFields2, fullFields2, fuzzy = true, sloppy = false, span = true, 2, 1), 1) //7
    // relaxed-shingle exact span matches

    // (queryBuilder(searchFields2, fullFields2, true, false, true, 2, 2), 1) //7
    // relaxed-shingle exact span matches
  )
}

class DealSearchRequestHandler(val config: Config, serverContext: SearchContext) extends Actor with Configurable with Logging {

  import DealSearchRequestHandler._

  private val esClient: Client = serverContext.esClient
  private var kwids: Array[String] = emptyStringArray
  private var w = emptyStringArray

  private def buildFilter(searchParams: DealSearchParams): BoolQueryBuilder = {
    import searchParams.filters._
    import searchParams.geo._
    import searchParams.idx._

    val finalFilter = boolQuery

    if (!kwids.isEmpty) {
      finalFilter.must(idsQuery(esType).addIds(kwids: _*))
    } else {
      finalFilter.must(termQuery("Published", 1l))
      if (applicableTo != "") {
        finalFilter.must(termQuery("ApplicableTo", applicableTo))
      }
      // Add area filters
      val locFilter = boolQuery
      if (area != "") {
        val areas: Array[String] = area.split( """,""").map(analyze(esClient, index, "Locations.Area.AreaExact", _).mkString(" ")).filter(!_.isEmpty)
        areas.map(fuzzyOrTermQuery("Locations.Area.AreaExact", _, 1f, 1, fuzzy = true)).foreach(a => locFilter should a)
        areas.map(fuzzyOrTermQuery("Locations.AreaSynonyms.AreaSynonymsExact", _, 1f, 1, fuzzy = true)).foreach(a => locFilter should a)
        areas.map(fuzzyOrTermQuery("Locations.City.CityExact", _, 1f, 1, fuzzy = true)).foreach(a => locFilter should a)
        areas.map(fuzzyOrTermQuery("Locations.CitySynonyms.CitySynonymsExact", _, 1f, 1, fuzzy = true)).foreach(a => locFilter should a)
        finalFilter.should(locFilter)
      }

      if (category != "") {
        val catFilter = boolQuery
        val categories: Array[String] = category.split( """,""").map(analyze(esClient, index, "Categories.Name.NameExact", _).mkString(" ")).filter(!_.isEmpty)
        categories.map(fuzzyOrTermQuery("Categories.Name.NameExact", _, 1f, 1, fuzzy = true)).foreach(a => catFilter should a)
        finalFilter.must(catFilter)
      }

      finalFilter.must(termQuery("Active", 1l))
      if (city != "") {
        val cityFilter = boolQuery
        city.split( """,""").map(analyze(esClient, index, "VisiblePlaces.City", _).mkString(" ")).filter(!_.isEmpty).foreach { c =>
          cityFilter.should(termQuery("VisiblePlaces.City", c))
          cityFilter.should(termQuery("VisiblePlaces.CitySynonyms", c))
          cityFilter.should(termQuery("DealDetail.VisibleToAllCities", true))
        }

        if (cityFilter.hasClauses)
          finalFilter.must(cityFilter)
      }
      if (featured == "true") {
        finalFilter.must(termQuery("IsFeatured", 1l))
      }
      if (dealsource != "") {
        finalFilter.must(termQuery("DealSource.Name", dealsource))
      }
    }
    finalFilter
  }

  private def buildSearch(searchParams: DealSearchParams): SearchRequestBuilder = {
    import searchParams.idx._
    import searchParams.limits._
    import searchParams.page._
    import searchParams.view._
    import searchParams.filters._

    var sort = "_score"
    if (screentype == "home") {
      sort = "_home,_score"
    }

    val sorters = getSort(sort)
    val search: SearchRequestBuilder = esClient.prepareSearch(index.split(","): _*)
      .setTypes(esType.split(","): _*)
      .setTrackScores(true)
      .setTimeout(TimeValue.timeValueMillis(Math.min(timeoutms, long("timeoutms"))))
      .setTerminateAfter(Math.min(maxdocspershard, int("max-docs-per-shard")))
      .setExplain(explain)
      .setSearchType(SearchType.fromString(searchType, ParseFieldMatcher.STRICT))
      .addSorts(sorters)
      .setFrom(offset).setSize(size)
    if (agg) {
      search.addAggregation(
        terms("categories").field("Categories.Name.NameAggr").size(aggbuckets).order(Terms.Order.aggregation("sum_score", false))
          .subAggregation(sum("sum_score").script(new Script("docscore", ScriptType.INLINE, "native", new util.HashMap[String, AnyRef]))))
    }
    if(select == "") {
      search.setFetchSource(source)
    } else {
      search.setFetchSource(select.split(""","""), unselect.split(""","""))
    }
    search
  }

  override def receive = {
    case searchParams: DealSearchParams =>
      try {
        import searchParams.text._
        import searchParams.idx._
        import searchParams.filters._
        import searchParams.startTime
        import searchParams.req._
        import searchParams.geo._

        kwids = id.split(",").map(_.trim.toUpperCase).filter(_.nonEmpty)
        w = if (kwids.length > 0) emptyStringArray else analyze(esClient, index, "Title", kw)
        if (w.length > 12) w = emptyStringArray
        w = w.take(8)
        if (w.isEmpty && kwids.isEmpty && category == "" && area == "" && screentype == "" &&
            city == "" && applicableTo == "" && featured == "" && dealsource == "") {
          context.parent ! EmptyResponse("empty search criteria")
        }
        val query =
          if (w.length > 0) qDefs.head._1(w, w.length)
          else matchAllQuery()
        val leastCount = qDefs.head._2
        val isMatchAll = query.isInstanceOf[MatchAllQueryBuilder]
        val finalFilter = buildFilter(searchParams)
        val search = buildSearch(searchParams)
        val me = context.self
        search.setQuery(boolQuery.must(query).filter(finalFilter))
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
      } catch {
        case e: Throwable =>
          context.parent ! ErrorResponse(e.getMessage, e)
      }

    case ReSearch(searchParams, filter, search, relaxLevel, response) =>
      try {
        import searchParams.startTime
        import searchParams.req._

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
      import response.result
      import response.searchParams.limits._
      import response.searchParams.req._
      import response.searchParams.startTime
      import response.relaxLevel
      try {
        val endTime = System.currentTimeMillis
        val timeTaken = endTime - startTime
        val parsedResult = parse(result.toString)
        info("[" + result.getTookInMillis + "/" + timeTaken + (if (result.isTimedOut) " timeout" else "") + "] [q" + relaxLevel + "] [" + result.getHits.hits.length + "/" + result.getHits.getTotalHits + (if (result.isTerminatedEarly) " termearly (" + Math.min(maxdocspershard, int("max-docs-per-shard")) + ")" else "") + "] [" + clip.toString + "]->[" + httpReq.uri + "]")
        context.parent ! SearchResult("", result.getHits.hits.length, timeTaken, relaxLevel, parsedResult)
      } catch {
        case e: Throwable =>
          context.parent ! ErrorResponse(e.getMessage, e)
      }
  }
}
