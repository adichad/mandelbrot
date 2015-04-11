package com.askme.mandelbrot.handler.aggregate

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.aggregate.message.{AggregateParams, AggregateResult, AggSpec}
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
import org.elasticsearch.search.aggregations.Aggregations
import org.elasticsearch.search.aggregations.bucket.terms.{Terms, TermsBuilder}
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality
import org.json4s._

import scala.collection.JavaConversions._


/**
  * Created by adichad on 08/01/15.
  */



class AggregateRequestHandler(val config: Config, serverContext: SearchContext) extends Actor with Configurable with Logging {
  private val esClient: Client = serverContext.esClient
  private def analyze(esClient: Client, index: String, field: String, text: String): Array[String] =
    new AnalyzeRequestBuilder(esClient.admin.indices, index, text).setField(field).get().getTokens.map(_.getTerm).toArray

  private def nestIfNeeded(fieldName: String, q: BaseQueryBuilder): BaseQueryBuilder = {
    val parts = fieldName.split(".")
    if (parts.length > 1)
      nestedQuery(parts(0), q).scoreMode("max")
    else q
  }

  private val aggregables = Map(
    "city"->"CityAggr",
    "loc"->"AreaAggr",
    "cat"->"Product.l3categoryaggr",
    "question"->"Product.stringattribute.qaggr",
    "answer"->"Product.stringattribute.aaggr")

  case object Aggregator {
    def apply(aggSpecs: Seq[AggSpec]) = {
      var aggDef: TermsBuilder = null
      aggSpecs.foreach { aggSpec =>
        val field = aggregables.get(aggSpec.name).getOrElse(aggSpec.name)
        val currAgg = terms(aggSpec.name).field(field).size(aggSpec.offset+aggSpec.size).shardSize(0)
        if(aggDef == null)
          aggDef = currAgg
        else
          aggDef = aggDef.subAggregation(currAgg)
      }
      aggDef
    }
  }

  private def buildSearch(aggParams: AggregateParams):Option[SearchRequestBuilder] = {
    import aggParams.agg._
    import aggParams.idx._
    import aggParams.lim._
    import aggParams.filter._


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
      if(b.hasClauses)
        query = filteredQuery(query, nestedFilter("Product", b).cache(false))

    }


    if (area != "") {
      val locFilter = boolFilter.cache(false)
      val analyzedAreas = scala.collection.mutable.Set[String]()
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
      if (locFilter.hasClauses)
        query = filteredQuery(query, locFilter)
    }

    if(question!="") {
      val questions = question.split("""#""")
      val b = boolFilter.cache(false)
      questions.foreach { c =>
        b.should(queryFilter(matchPhraseQuery("Product.stringattribute.question", c)).cache(false))
      }
      if(b.hasClauses)
        query = filteredQuery(query, b.cache(false))
      debug(b)
    }

    if(answer!="") {
      val answers = answer.split("""#""")
      val b = boolFilter.cache(false)
      answers.foreach { c =>
        b.should(queryFilter(matchPhraseQuery("Product.stringattribute.answer", c)).cache(false))
      }
      if(b.hasClauses)
        query = filteredQuery(query, b.cache(false))
      debug(b)
    }

    val aggregator = Aggregator(aggSpecs)

    val search = esClient.prepareSearch(index.split(","): _*).setQueryCache(false)
      .setTypes(esType.split(","): _*)
      .setSearchType(SearchType.COUNT)
      .setQuery(filteredQuery(query, boolFilter.mustNot(termFilter("DeleteFlag", 1l))))
      .setTrackScores(false)
      .setFrom(0).setSize(0)
      .setTimeout(TimeValue.timeValueMillis(Math.min(timeoutms, long("timeoutms"))))
      .setTerminateAfter(Math.min(maxdocspershard, int("max-docs-per-shard")))
      .addAggregation(aggregator)

    aggSpecs.foreach(s=>
      search.addAggregation(
        cardinality(s.name+"Count").field(aggregables.get(s.name).getOrElse(s.name)).precisionThreshold(40000)
      )
    )

    return Some(search)
  }


  case class WrappedResponse(aggParams: AggregateParams, result: SearchResponse)

  override def receive = {
    case aggParams: AggregateParams =>
      buildSearch(aggParams) match {
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
        import response.aggParams.lim._
        import response.aggParams.req._
        import response.aggParams.startTime
        import response.result

        val recordCount = result.getHits.totalHits
        val res = reshape(response)

        val timeTaken = System.currentTimeMillis - startTime
        info("[" + result.getTookInMillis + "/" + timeTaken + (if(result.isTimedOut) " timeout" else "") + "] [" + recordCount + (if(result.isTerminatedEarly) " termearly ("+Math.min(maxdocspershard, int("max-docs-per-shard"))+")" else "") + "] [" + clip.toString + "]->[" + httpReq.uri + "]")

        context.parent ! AggregateResult(recordCount, timeTaken, result.isTerminatedEarly, result.isTimedOut, res)
  }

  private def reshape(response: WrappedResponse) = {

    def reshape(result: Aggregations, aggSpecs: Seq[AggSpec], cardinalities: Seq[Long]): JObject = {
      if(aggSpecs.size>0) {
        val agg = aggSpecs(0)
        val currAgg = result.get(agg.name).asInstanceOf[Terms]
        val buckets = currAgg.getBuckets.drop(agg.offset)
        JObject(
          JField(agg.name,
            JObject(
              JField("group-count", JInt(buckets.size)),
              JField("total-group-count", JInt(cardinalities(0))),
              JField("buckets",
                JObject(buckets.map( x=>
                  JField(x.getKey,
                    if(aggSpecs.size>1)
                      JObject(
                        JField("count", JInt(x.getDocCount)),
                        JField("sub", reshape(x.getAggregations, aggSpecs.drop(1), cardinalities.drop(1)))
                      )
                    else
                      JInt(x.getDocCount)
                  )
                ).toList)
              )
            )
          )
        )
      }
      else
        JObject()
    }

    val cardinalities = response.aggParams.agg.aggSpecs.map(s=>response.result.getAggregations.get(s.name+"Count").asInstanceOf[Cardinality].getValue)
    reshape(response.result.getAggregations, response.aggParams.agg.aggSpecs, cardinalities)

  }
}

