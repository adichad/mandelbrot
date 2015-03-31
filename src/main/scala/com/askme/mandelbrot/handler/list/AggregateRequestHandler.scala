package com.askme.mandelbrot.handler.list

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.list.message.{AggregateParams, AggregateResult}
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse, SearchType}
import org.elasticsearch.client.Client
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.BaseQueryBuilder
import org.elasticsearch.index.query.FilterBuilders._
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.parsing.json.JSONArray


/**
  * Created by adichad on 08/01/15.
  */



class AggregateRequestHandler(val config: Config, serverContext: SearchContext) extends Actor with Configurable with Logging {
  private val esClient: Client = serverContext.esClient
  private def analyze(esClient: Client, index: String, field: String, text: String): Array[String] =
    new AnalyzeRequestBuilder(esClient.admin.indices, index, text).setField(field).get().getTokens.map(_.getTerm).toArray

  private def buildSearch(aggParams: AggregateParams):Option[SearchRequestBuilder] = {
    import aggParams.agg._
    import aggParams.idx._
    import aggParams.lim._
    import aggParams.page._


    var query: BaseQueryBuilder = matchAllQuery

    // filters
    val cityFilter = boolFilter.cache(false)
    if (city != "") {
      val cityParams = city.split( """#""").map(_.trim.toLowerCase)
      cityFilter.should(termsFilter("City", cityParams: _*).cache(false))
      cityFilter.should(termsFilter("CitySynonyms", cityParams: _*).cache(false))
      cityFilter.should(termsFilter("CitySlug", cityParams: _*).cache(false))
      query = filteredQuery(query, cityFilter)
    }

     if (category != "") {
       val cats = category.split("""#""")
       val b = boolFilter.cache(false)
       cats.foreach { c =>
         b.should(queryFilter(matchPhraseQuery("Product.l3category", c)).cache(false))
         b.should(termFilter("Product.l3categoryslug", c).cache(false))
       }
       query = filteredQuery(query, nestedFilter("Product", b).cache(false))
     }

     val locFilter = boolFilter.cache(false)
     val analyzedAreas = scala.collection.mutable.Set[String]()
     if (area != "") {
       val areas = area.split( """,""").map(_.trim.toLowerCase)
       areas.foreach { a =>

         val areaWords = analyze(esClient, index, "Area", a)
         val terms = areaWords
           .map(spanTermQuery("Area", _))
         val areaSpan = spanNearQuery.slop(0).inOrder(true)
         terms.foreach(areaSpan.clause)
         locFilter.should(queryFilter(areaSpan).cache(false))

         val synTerms = areaWords
           .map(spanTermQuery("AreaSynonyms", _))
         val synAreaSpan = spanNearQuery.slop(0).inOrder(true)
         synTerms.foreach(synAreaSpan.clause)
         locFilter.should(queryFilter(synAreaSpan).cache(false))
       }


       areas.map(a => termFilter("AreaSlug", a).cache(false)).foreach(locFilter.should)

     }

     if (locFilter.hasClauses)
       query = filteredQuery(query, locFilter)


     val search = esClient.prepareSearch(index.split(","): _*).setQueryCache(false)
       .setTypes(esType.split(","): _*)
       .setSearchType(SearchType.COUNT)
       .setQuery(filteredQuery(query, boolFilter.mustNot(termFilter("DeleteFlag", 1l))))
       .setTrackScores(false)
       .setFrom(0).setSize(0)
       .setTimeout(TimeValue.timeValueMillis(Math.min(timeoutms, long("timeoutms"))))
       .setTerminateAfter(Math.min(maxdocspershard, int("max-docs-per-shard")))
       .addAggregation(terms(agg).field(agg).size(offset+size).shardSize(0))

     return Some(search)
   }


   case class WrappedResponse(aggParams: AggregateParams, result: SearchResponse)

   override def receive = {
     case aggParams: AggregateParams =>

       val searchOpt = buildSearch(aggParams)
       searchOpt match {
       case None=>
       case Some(search) =>
         val me = context.self

         search.execute(new ActionListener[SearchResponse] {
           override def onResponse(response: SearchResponse): Unit = {
             me ! WrappedResponse(aggParams, response)
           }

           override def onFailure(e: Throwable): Unit = {
             throw e
           }
         })
     }
     case response: WrappedResponse =>
       import response.aggParams.agg._
       import response.aggParams.lim._
       import response.aggParams.page._
       import response.aggParams.req._
       import response.aggParams.startTime
       import response.result

       val buckets = result.getAggregations.get(agg).asInstanceOf[Terms].getBuckets
       val bucks = buckets.drop(offset)
       val res = new JSONArray(bucks.toList)
       val count = buckets.size
       val resCount = bucks.size

       val timeTaken = System.currentTimeMillis - startTime
       info("[" + result.getTookInMillis + "/" + timeTaken + (if(result.isTimedOut) " timeout" else "") + "] [" + count + "/" + res.size + (if(result.isTerminatedEarly) " termearly ("+Math.min(maxdocspershard, int("max-docs-per-shard"))+")" else "") + "] [" + clip.toString + "]->[" + httpReq.uri + "]")

       context.parent ! AggregateResult(count, resCount, timeTaken, parse(res.toString))
   }

 }

