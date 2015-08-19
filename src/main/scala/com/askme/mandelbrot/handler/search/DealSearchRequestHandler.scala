package com.askme.mandelbrot.handler.search

import java.net.URLEncoder

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.EmptyResponse
import com.askme.mandelbrot.handler.message.ErrorResponse
import com.askme.mandelbrot.handler.search.message.{SearchResult, DealSearchParams}
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder
import org.elasticsearch.action.search.{SearchType, SearchRequestBuilder, SearchResponse}
import org.elasticsearch.client.Client
import org.elasticsearch.common.unit.{TimeValue, Fuzziness}
import org.elasticsearch.index.query._
import org.elasticsearch.index.query.FilterBuilders._
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.search.sort.{FieldSortBuilder, ScoreSortBuilder, SortOrder, SortBuilders}
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConversions._

/**
 * Created by nishantkumar on 7/30/15.
 */

object DealSearchRequestHandler extends Logging {

  private def analyze(esClient: Client, index: String, field: String, text: String): Array[String] =
    new AnalyzeRequestBuilder(esClient.admin.indices, index, text).setField(field).get().getTokens.map(_.getTerm).toArray

  private implicit class DisMaxQueryPimp(val q: DisMaxQueryBuilder) {
    def addAll(queries: Iterable[QueryBuilder]) = {
      queries.foreach(q.add)
      q
    }
  }

  private[DealSearchRequestHandler] def nestIfNeeded(fieldName: String, q: BaseQueryBuilder): BaseQueryBuilder = {
    /*val parts = fieldName.split("""\.""")
    if (parts.length > 1)
      nestedQuery(parts(0), q).scoreMode("max")
    else
    */
    q
  }

  private def superBoost(len: Int) = math.pow(10, math.min(10,len+1)).toFloat
  private case class WrappedResponse(searchParams: DealSearchParams, result: SearchResponse, relaxLevel: Int)
  private case class ReSearch(searchParams: DealSearchParams, filter: FilterBuilder, search: SearchRequestBuilder, relaxLevel: Int, response: SearchResponse)

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
    "TitleExact"->100000000000f, "HeadlineExact"->100000000000f,
    "Categories.NameExact"->10000000000f,
    "Categories.SynonymExact"->10000000f,
    "DealTagsExact"->10000000f,
    "Offers.NameExact"->1000f,
    "DescriptionShortExact"->1000f,
    "Offers.DescriptionShortExact" -> 1000f,
    "Locations.AreaExact"->10f, "Locations.AreaSynonymsExact"->10f,
    "Locations.CityExact"->1f, "Locations.CitySynonymsExacts"->1f)

  private[DealSearchRequestHandler] def shingleSpan(field: String, boost: Float, w: Array[String], fuzzyprefix: Int, maxShingle: Int, minShingle: Int = 1, sloppy: Boolean = true, fuzzy: Boolean = true) = {
    val fieldQuery1 = boolQuery.minimumShouldMatch("33%")
    val terms: Array[BaseQueryBuilder with SpanQueryBuilder] = w.map(x=>
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

  private def queryBuilder(tokenFields: Map[String, Float], recomFields: Map[String, Float], fuzzy: Boolean = false, sloppy: Boolean = false, span: Boolean = false, minShingle: Int = 1, tokenRelax: Int = 0)
                          (w: Array[String], maxShingle: Int) = {
    shinglePartition(tokenFields, recomFields, w, maxShingle, minShingle, fuzzy, sloppy, span, tokenRelax)
  }

  private val qDefs: Seq[((Array[String], Int)=>BaseQueryBuilder, Int)] = Seq(

    //                                        fuzzy, slop,  span, minshingle, tokenrelax
    (queryBuilder(searchFields2, fullFields2, false, false, false, 1, 0), 1), //1
    // full-shingle exact full matches

    (queryBuilder(searchFields2, fullFields2, true, false, false, 1, 0), 1), //3
    // full-shingle fuzzy full matches

    //(queryBuilder(searchFields2, fullFields2, false, false, true, 2, 0), 1), //1
    // full-shingle exact span matches

    (queryBuilder(searchFields2, fullFields2, false, true, true, 2, 0), 1), //5
    // full-shingle exact sloppy-span matches

    (queryBuilder(searchFields2, fullFields2, false, false, false, 1, 1), 1), //6
    // relaxed-shingle exact full matches

    (queryBuilder(searchFields2, fullFields2, false, false, true, 2, 1), 1), //7
    // relaxed-shingle exact span matches

    (queryBuilder(searchFields2, fullFields2, false, false, true, 2, 2), 1) //7
    // relaxed-shingle exact span matches


  )
}

class DealSearchRequestHandler(val config: Config, serverContext: SearchContext) extends Actor with Configurable with Logging {

  import DealSearchRequestHandler._

  private val esClient: Client = serverContext.esClient
  private var kwids: Array[String] = emptyStringArray
  private var w = emptyStringArray

  private def buildFilter(searchParams: DealSearchParams): FilterBuilder = {
    import searchParams.filters._
    import searchParams.geo._
    import searchParams.idx._

    val finalFilter = andFilter().cache(false)

    if (!kwids.isEmpty) {
      finalFilter.add(idsFilter(esType).addIds(kwids: _*))
    } else {
      finalFilter.add(boolFilter.must(termFilter("Published", 1l).cache(false)))
      if (applicableTo != "") {
        finalFilter.add(termFilter("ApplicableTo", applicableTo).cache(false))
      }
      // Add area filters
      val locFilter = boolFilter.cache(false)
      if (area != "") {
        val areas: Array[String] = area.split( """,""").map(analyze(esClient, index, "Locations.Area.AreaExact", _).mkString(" ")).filter(!_.isEmpty)
        areas.map(fuzzyOrTermQuery("Locations.Area.AreaExact", _, 1f, 1, true)).foreach(a => locFilter should queryFilter(a).cache(false))
        areas.map(fuzzyOrTermQuery("Locations.AreaSynonyms.AreaSynonymsExact", _, 1f, 1, true)).foreach(a => locFilter should queryFilter(a).cache(false))
        areas.map(fuzzyOrTermQuery("Locations.City.CityExact", _, 1f, 1, true)).foreach(a => locFilter should queryFilter(a).cache(false))
        areas.map(fuzzyOrTermQuery("Locations.CitySynonyms.CitySynonymsExact", _, 1f, 1, true)).foreach(a => locFilter should queryFilter(a).cache(false))
        finalFilter.add(locFilter)
      }

      finalFilter.add(andFilter(boolFilter.must(termFilter("Active", 1l).cache(false))).cache(false))
      if (city != "") {
        val cityFilter = boolFilter.cache(false)
        city.split( """,""").map(analyze(esClient, index, "Locations.City", _).mkString(" ")).filter(!_.isEmpty).foreach { c =>
          cityFilter.should(termFilter("Locations.City", c).cache(false))
          cityFilter.should(termFilter("Locations.CitySynonyms", c).cache(false))
        }

        if (cityFilter.hasClauses)
          finalFilter.add(cityFilter)
      }
    }
    finalFilter
  }

  private def buildSearch(searchParams: DealSearchParams): SearchRequestBuilder = {
    import searchParams.idx._
    import searchParams.limits._
    import searchParams.page._
    import searchParams.view._

    val search: SearchRequestBuilder = esClient.prepareSearch(index.split(","): _*).setQueryCache(false)
      .setTypes(esType.split(","): _*)
      .setTrackScores(true)
      .setTimeout(TimeValue.timeValueMillis(Math.min(timeoutms, long("timeoutms"))))
      .setTerminateAfter(Math.min(maxdocspershard, int("max-docs-per-shard")))
      .setExplain(explain)
      .setSearchType(SearchType.fromString(searchType))
      .setFrom(offset).setSize(size)
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

        kwids = id.split(",").map(_.trim.toUpperCase).filter(_.nonEmpty)
        w = if (kwids.length > 0) emptyStringArray else analyze(esClient, index, "Title", kw)
        if (w.length > 12) w = emptyStringArray
        // If we are looking for Idea, Airtel etc deal then do not make w mandatory.
        if (w.isEmpty && kwids.isEmpty && applicableTo == "default") {
          context.parent ! EmptyResponse("empty search criteria")
        }
        else {
          val query =
            if (w.length > 0) qDefs(0)._1(w, w.length)
            else matchAllQuery()
          val leastCount = qDefs(0)._2
          val isMatchAll = query.isInstanceOf[MatchAllQueryBuilder]
          val finalFilter = buildFilter(searchParams)
          val search = buildSearch(searchParams)
          val me = context.self
          search.setQuery(filteredQuery(query, finalFilter))
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
        import searchParams.startTime
        import searchParams.req._

        if (relaxLevel >= qDefs.length)
          context.self ! WrappedResponse(searchParams, response, relaxLevel - 1)
        else {
          val query = qDefs(relaxLevel)._1(w, w.length)
          val leastCount = qDefs(relaxLevel)._2
          val me = context.self

          search.setQuery(filteredQuery(query, filter)).execute(new ActionListener[SearchResponse] {
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
