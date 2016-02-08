package com.askme.mandelbrot.handler.get

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.get.message.GetParams
import com.askme.mandelbrot.handler.message.ErrorResponse
import com.askme.mandelbrot.handler.search.message.SuggestResult
import com.askme.mandelbrot.handler.suggest.message.SuggestParams
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.analyze.{AnalyzeAction, AnalyzeRequestBuilder}
import org.elasticsearch.action.get.GetResponse
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


class GetRequestHandler(val config: Config, serverContext: SearchContext) extends Actor with Configurable with Logging {
  private val esClient: Client = serverContext.esClient


  override def receive = {

    case getParams: GetParams =>
      import getParams._
      try {
        val getter =
          esClient
            .prepareGet(string(s"mappings.$esType.index"), esType, id)
            .setFetchSource(select, null).setRealtime(true).setTransformSource(transform)
            .execute(new ActionListener[GetResponse] {
              override def onResponse(response: GetResponse): Unit = {
                val res = parse(response.toString)
                val endTime = System.currentTimeMillis
                val timeTaken = endTime - startTime
                info("[" + timeTaken + "] [" + clip.toString + "]->[" + req.uri + "]")
                context.parent ! SuggestResult(timeTaken, res)
              }

              override def onFailure(e: Throwable): Unit = {
                val timeTaken = System.currentTimeMillis() - startTime
                error("[" + timeTaken + "] ["+e.getMessage+"] [" + clip.toString + "]->[" + req.uri + "]", e)
                context.parent ! ErrorResponse(e.getMessage, e)
              }

            })
      } catch {
        case e: Throwable =>
          val timeTaken = System.currentTimeMillis() - startTime
          error("[" + timeTaken + "] ["+e.getMessage+"] [" + clip.toString + "]->[" + req.uri + "]", e)
          context.parent ! ErrorResponse(e.getMessage, e)
      }

  }

}

