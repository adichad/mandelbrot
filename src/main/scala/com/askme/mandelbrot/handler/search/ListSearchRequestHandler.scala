package com.askme.mandelbrot.handler.search

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.search.message.{SearchParams, SearchResult}
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.analyze.{AnalyzeAction, AnalyzeRequestBuilder}
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse, SearchType}
import org.elasticsearch.client.Client
import org.elasticsearch.common.ParseFieldMatcher
import org.elasticsearch.common.unit.{Fuzziness, TimeValue}
import org.elasticsearch.index.query.SpanQueryBuilder
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.search.sort.SortOrder
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConversions._

/**
 * Created by adichad on 09/05/15.
 */


object ListSearchRequestHandler extends Logging {

  val pat = """(?U)[^\p{alnum}]+"""

  private[ListSearchRequestHandler] def nestIfNeeded(fieldName: String, q: QueryBuilder): QueryBuilder = {
    /*val parts = fieldName.split("""\.""")
    if (parts.length > 1)
      nestedQuery(parts(0), q).scoreMode("max")
    else
    */
    q
  }


  private[ListSearchRequestHandler] def shingleSpan(field: String, boost: Float, w: Array[String], fuzzyprefix: Int, fuzzysim: Float, maxShingle: Int, minShingle: Int = 1, sloppy: Boolean = true) = {
    val fieldQuery1 = boolQuery.minimumShouldMatch("67%")

    val terms: Array[SpanQueryBuilder] = w.map(x=>
      if(x.length > 3)
        spanMultiTermQueryBuilder(
          fuzzyQuery(field, x).prefixLength(fuzzyprefix).fuzziness(if(x.length > 6) Fuzziness.TWO else Fuzziness.ONE))
      else
        spanTermQuery(field, x)
    )
    
    (minShingle to Math.min(terms.length, maxShingle)).foreach { len =>
      val slop = if(sloppy)math.max(0, len - 2) else 0
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

  private[ListSearchRequestHandler] def shingleFull(field: String, boost: Float, w: Array[String], fuzzyprefix: Int, maxShingle: Int, minShingle: Int = 1) = {
    val fieldQuery = boolQuery
    (minShingle to math.min(maxShingle, w.length)).foreach { len =>
      w.sliding(len).foreach { shingle =>
        val x = shingle.mkString(" ")
        fieldQuery.should(
          if(x.length > 3)
            fuzzyQuery(field, x).prefixLength(fuzzyprefix).fuzziness(if(x.length>6) Fuzziness.TWO else Fuzziness.ONE)
          else
            termQuery(field, x))
      }
    }
    nestIfNeeded(field, fieldQuery)
  }

  private def analyze(esClient: Client, index: String, field: String, text: String): Array[String] = {
    new AnalyzeRequestBuilder(esClient.admin.indices, AnalyzeAction.INSTANCE, index, text).setField(field).get().getTokens.map(_.getTerm).toArray
  }

  private val searchFields = Map("label" -> 0f, "keywords" -> 0f, "content_title" -> 0f)

  private val fullFields = Map("labelexact" -> 0f, "keywordsexact"->0f,"content_titleexact"->0f)

  private val shingleFields = Map("labelshingle" -> 0f, "keywordsshingle"->0f,"content_titleshingle"->0f)

  private val emptyStringArray = new Array[String](0)

}

class ListSearchRequestHandler(val config: Config, serverContext: SearchContext) extends Actor with Configurable with Logging {
  import ListSearchRequestHandler._
  private val esClient: Client = serverContext.esClient
  private var w = emptyStringArray


  private def buildSearch(searchParams: SearchParams): SearchRequestBuilder = {
    import searchParams.idx._
    import searchParams.limits._
    import searchParams.page._
    import searchParams.text._
    import searchParams.view._


    var query: QueryBuilder = matchAllQuery()

    if (kw != null && kw.trim != "") {
      w = analyze(esClient, index, "label", kw)
      if (w.length > 0) {
        val kwquery = disMaxQuery

        if(w.length > 1) {
          searchFields.foreach {
            field: (String, Float) => {
              kwquery.add(shingleSpan(field._1, field._2, w, 2, fuzzysim, math.min(4, w.length), 2))
            }
          }
        }

        fullFields.foreach {
          field: (String, Float) => {
            kwquery.add(shingleFull(field._1, field._2, w, 2, 5, 1))
          }
        }

        shingleFields.foreach {
          field: (String, Float) => {
            kwquery.add(shingleFull(field._1, field._2, w, 2, 3, 1))
          }
        }
        query = kwquery
      }
    }

    esClient.prepareSearch(index.split(","): _*)
      .setTypes(esType.split(","): _*)
      .setSearchType(SearchType.fromString(searchType, ParseFieldMatcher.STRICT))
      .setQuery(query)
      .setTrackScores(false)
      .setFrom(offset).setSize(size)
      .setTimeout(TimeValue.timeValueMillis(Math.min(timeoutms, long("timeoutms"))))
      .setTerminateAfter(Math.min(maxdocspershard, int("max-docs-per-shard")))
      .setExplain(explain)
      .setFetchSource(select.split(""","""), unselect.split(""",""")).addSort("id", SortOrder.ASC)
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

        override def onFailure(e: Throwable): Unit = {
          throw e
        }
      })
    case response: WrappedResponse =>
      import response.result
      import response.searchParams.limits._
      import response.searchParams.req._
      import response.searchParams.startTime
      val endTime = System.currentTimeMillis
      val timeTaken = endTime - startTime
      info("[" + result.getTookInMillis + "/" + timeTaken + (if(result.isTimedOut) " timeout" else "") + "] [" + result.getHits.hits.length + "/" + result.getHits.getTotalHits + (if(result.isTerminatedEarly) " termearly ("+Math.min(maxdocspershard, int("max-docs-per-shard"))+")" else "") + "] [" + clip.toString + "]->[" + httpReq.uri + "]")
      context.parent ! SearchResult("", result.getHits.hits.length, timeTaken, 0, parse(result.toString))
  }

}

