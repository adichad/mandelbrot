package com.askme.mandelbrot.handler.search

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.search.message.{SearchParams, SearchResult}
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse, SearchType}
import org.elasticsearch.client.Client
import org.elasticsearch.common.unit.{Fuzziness, TimeValue}
import org.elasticsearch.index.query.BaseQueryBuilder
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.search.sort.SortOrder
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConversions._

/**
 * Created by adichad on 09/05/15.
 */


object ListSearchRequestHandler extends Logging {

  val pat = """(?U)[^\p{alnum}]+"""

  private[ListSearchRequestHandler] def nestIfNeeded(fieldName: String, q: BaseQueryBuilder): BaseQueryBuilder = {
    /*val parts = fieldName.split("""\.""")
    if (parts.length > 1)
      nestedQuery(parts(0), q).scoreMode("max")
    else
    */
    q
  }


  private[ListSearchRequestHandler] def shingleSpan(field: String, boost: Float, w: Array[String], fuzzyprefix: Int, fuzzysim: Float, maxShingle: Int, minShingle: Int = 1, sloppy: Boolean = true) = {
    val fieldQuery1 = boolQuery.minimumShouldMatch("80%")

    val terms = w
      .map(fuzzyQuery(field, _).prefixLength(fuzzyprefix).fuzziness(Fuzziness.ONE))
      .map(spanMultiTermQueryBuilder)

    (minShingle to Math.min(terms.length, maxShingle)).foreach { len =>
      val slop = if(sloppy)math.max(0,len - 2) else 0
      terms.sliding(len).foreach { shingle =>
        val nearQuery = spanNearQuery.slop(if(sloppy)math.max(0,math.min(1,len - 3)) else 0).inOrder(!sloppy)
        shingle.foreach(nearQuery.clause)
        fieldQuery1.should(nearQuery)
      }
    }
    nestIfNeeded(field, fieldQuery1)
  }

  private def analyze(esClient: Client, index: String, field: String, text: String): Array[String] =
    new AnalyzeRequestBuilder(esClient.admin.indices, index, text).setField(field).get().getTokens.map(_.getTerm).toArray


  private val searchFields = Map("label" -> 0f, "keywords" -> 0f, "content.title" -> 0f)

  private val fullFields = Map("labelexact" -> 0f, "keywordsexact"->0f,"content.titleexact"->0f)

  private val shingleFields = Map("labelshingle" -> 0f, "keywordsshingle"->0f,"content.titleshingle"->0f)

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


    var query: BaseQueryBuilder = matchAllQuery()

    if (kw != null && kw.trim != "") {
      w = analyze(esClient, index, "label", kw)
      if (w.length > 0) {
        val kwquery = disMaxQuery

        searchFields.foreach {
          field: (String, Float) => {
            kwquery.add(shingleSpan(field._1, field._2, w, 2, fuzzysim,
              math.min(4, w.length), //max-shingle
              math.max(1, math.min(w.length/2, math.min(4, w.length))))) //min-shingle
          }
        }

        fullFields.foreach {
          field: (String, Float) => {
            (math.max(w.length/2, 1) to w.length).foreach { len =>
              w.sliding(len).foreach { shingle =>
                val k = shingle.mkString(" ")
                kwquery.add(nestIfNeeded(field._1, termQuery(field._1, k)))
              }
            }
          }
        }

        shingleFields.foreach {
          field: (String, Float) => {
            (math.max(w.length/2, 1) to w.length).foreach { len =>
              w.sliding(len).foreach { shingle =>
                val k = shingle.mkString(" ")
                kwquery.add(nestIfNeeded(field._1, termQuery(field._1, k)))
              }
            }
          }
        }
        query = kwquery
      }
    }

    esClient.prepareSearch(index.split(","): _*).setQueryCache(false)
      .setTypes(esType.split(","): _*)
      .setSearchType(SearchType.fromString(searchType))
      .setQuery(query)
      .setTrackScores(true)
      .addFields(select.split( ""","""): _*)
      .setFrom(offset).setSize(size)
      .setTimeout(TimeValue.timeValueMillis(Math.min(timeoutms, long("timeoutms"))))
      .setTerminateAfter(Math.min(maxdocspershard, int("max-docs-per-shard")))
      .setExplain(explain)
      .setFetchSource(select.split(""","""), null).addSort("id", SortOrder.ASC)
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
      context.parent ! SearchResult("", "", result.getHits.hits.length, timeTaken, parse(result.toString))
  }

}

