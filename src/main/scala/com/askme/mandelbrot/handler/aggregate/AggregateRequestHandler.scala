package com.askme.mandelbrot.handler.aggregate

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.aggregate.message.{AggSpec, AggregateParams, AggregateResult}
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.analyze.{AnalyzeAction, AnalyzeRequestBuilder}
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse}
import org.elasticsearch.client.Client
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.aggregations.bucket.nested.{Nested, NestedBuilder, ReverseNested, ReverseNestedBuilder}
import org.elasticsearch.search.aggregations.bucket.terms.{Terms, TermsBuilder}
import org.elasticsearch.search.aggregations.{Aggregation, AggregationBuilder, Aggregations}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.language.existentials
import scala.collection.JavaConversions._


/**
  * Created by adichad on 08/01/15.
  */



class AggregateRequestHandler(val config: Config, serverContext: SearchContext) extends Actor with Configurable with Logging {
  private val esClient: Client = serverContext.esClient
  private def analyze(esClient: Client, index: String, field: String, text: String): Array[String] =
    new AnalyzeRequestBuilder(esClient.admin.indices, AnalyzeAction.INSTANCE, index, text).setField(field).get().getTokens.map(_.getTerm).toArray

  private def nestIfNeeded(fieldName: String, q: QueryBuilder): QueryBuilder = {
    val parts = fieldName.split(".")
    if (parts.length > 1)
      nestedQuery(parts(0), q).scoreMode("max")
    else q
  }

  private val aggregables = Map(
    "city"->"CityAggr",
    "loc"->"AreaAggr",
    "cat3"->"Product.l3categoryaggr",
    "cat2"->"Product.l2categoryaggr",
    "cat1"->"Product.l1categoryaggr",
    "ckw"->"Product.categorykeywordsaggr",
    "name"->"LocationNameAggr",
    "question"->"Product.stringattribute.qaggr",
    "answer"->"Product.stringattribute.aaggr")

  case object Aggregator {
    def apply(aggSpecs: Seq[AggSpec]) = {
      var path = ""

      type AB = AggregationBuilder[_ >: TermsBuilder with NestedBuilder with ReverseNestedBuilder <: AggregationBuilder[_ >: TermsBuilder with NestedBuilder with ReverseNestedBuilder]]

      var aggDef: AB = null
      var prev: AB = null
      aggSpecs.foreach { aggSpec: AggSpec =>
        val field = aggregables.getOrElse(aggSpec.name, aggSpec.name)
        val newpath = nestPath(field)

        val lcpp = longestCommonPathPrefix(newpath, path)
        val curr = currAgg(aggSpec, field)
        val agg =
          if (lcpp == path)
            if (newpath == path) curr
            else nested(aggSpec.name).path(newpath).subAggregation(curr)
          else reverseNested(aggSpec.name).path(lcpp).subAggregation(nested(aggSpec.name).path(newpath).subAggregation(curr))
        path = newpath
        if(aggDef == null)
          aggDef = agg
        else {
          prev.subAggregation(agg)
          //prev.subAggregation(currCountAgg(aggSpec, field))
        }
        prev = curr

      }
      aggDef
    }

    def longestCommonPathPrefix(a: String, b: String) = {
      a.split("""\.""").zip(b.split("""\.""")).takeWhile(x=>x._1==x._2).map(x=>x._1).mkString(".")
    }

    def currCountAgg(aggSpec: AggSpec, field: String) = {
      cardinality(aggSpec.name).field(field).precisionThreshold(40000)
    }
    def currAgg(aggSpec: AggSpec, field: String) = {

      terms(aggSpec.name).field(field).size(aggSpec.offset+aggSpec.size).shardSize(0)
    }

    def nestPath(field: String) = {
      val parts = field.split("""\.""")
      parts.take(math.max(0,parts.length-1)).mkString(".")
    }
    def nestPath(aggSpec: AggSpec) = {
      val parts = aggregables.getOrElse(aggSpec.name, aggSpec.name).split("""\.""")
      parts.take(math.max(0, parts.length - 1)).mkString(".")
    }
    def nestPath(aggSpecs: Seq[AggSpec]) = {
      val parts = aggSpecs.filter(s => aggregables.getOrElse(s.name, s.name).contains("."))
        .map(s => aggregables.getOrElse(s.name, s.name)).headOption.getOrElse("").split("""\.""")

      parts.take(parts.length - 1).mkString(".")
    }
  }



  private def buildSearch(aggParams: AggregateParams):Option[SearchRequestBuilder] = {
    import aggParams.agg._
    import aggParams.filter._
    import aggParams.idx._
    import aggParams.lim._


    val query = boolQuery

    // filters
    val cityFilter = boolQuery
    if (city != "") {
      val cityParams = city.split( """#""").map(_.trim.toLowerCase)
      cityFilter.should(termsQuery("City", cityParams: _*))
      cityFilter.should(termsQuery("CitySynonyms", cityParams: _*))
      cityFilter.should(termsQuery("CitySlug", cityParams: _*))
      query.must(cityFilter)
    }

    if (category != "") {
      val cats = category.split("""#""")
      val b = boolQuery
      cats.foreach { c =>
        b.should(matchPhraseQuery("Product.l3category", c))
        b.should(termQuery("Product.l3categoryslug", c))
      }
      if(b.hasClauses)
        query.must(nestedQuery("Product", b))

    }


    if (area != "") {
      val locFilter = boolQuery
      //val analyzedAreas = scala.collection.mutable.Set[String]()
      val areas = area.split( """,""").map(_.trim.toLowerCase)
      areas.foreach { a =>
        val areaWords = analyze(esClient, index, "Area", a)
        val terms = areaWords
          .map(spanTermQuery("Area", _))
        val areaSpan = spanNearQuery.slop(0).inOrder(true)
        terms.foreach(areaSpan.clause)
        locFilter.should(areaSpan)

        val synTerms = areaWords
          .map(spanTermQuery("AreaSynonyms", _))
        val synAreaSpan = spanNearQuery.slop(0).inOrder(true)
        synTerms.foreach(synAreaSpan.clause)
        locFilter.should(synAreaSpan)
      }
      areas.map(a => termQuery("AreaSlug", a)).foreach(locFilter.should)
      if (locFilter.hasClauses)
        query.must(locFilter)
    }

    if(question!="") {
      val questions = question.split("""#""")
      val b = boolQuery
      questions.foreach { c =>
        b.should(matchPhraseQuery("Product.stringattribute.question", c))
      }
      if(b.hasClauses)
        query.must(b)
      debug(b)
    }

    if(answer!="") {
      val answers = answer.split("""#""")
      val b = boolQuery
      answers.foreach { c =>
        b.should(matchPhraseQuery("Product.stringattribute.answer", c))
      }
      if(b.hasClauses)
        query.must(b)
      debug(b)
    }

    val aggregator = Aggregator(aggSpecs)

    val search = esClient.prepareSearch(index.split(","): _*)
      .setTypes(esType.split(","): _*)
      .setQuery(query.mustNot(termQuery("DeleteFlag", 1l)))
      .setTrackScores(false)
      .setFrom(0).setSize(0)
      .setTimeout(TimeValue.timeValueMillis(Math.min(timeoutms, long("timeoutms"))))
      .setTerminateAfter(Math.min(maxdocspershard, int("max-docs-per-shard")))
      .addAggregation(aggregator)

    //TODO: nested?
    aggSpecs.foreach(s=>
      search.addAggregation(
        cardinality(s.name+"Count").field(aggregables.getOrElse(s.name, s.name)).precisionThreshold(40000)
      )
    )

    Some(search)
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
        val res: JValue = if(response.aggParams.agg.response == "processed") reshape(response) else parse(result.toString)

        val timeTaken = System.currentTimeMillis - startTime
        info("[" + result.getTookInMillis + "/" + timeTaken + (if(result.isTimedOut) " timeout" else "") + "] [" + recordCount + (if(result.isTerminatedEarly) " termearly ("+Math.min(maxdocspershard, int("max-docs-per-shard"))+")" else "") + "] [" + clip.toString + "]->[" + httpReq.uri + "]")
        parse(response.result.toString)
        context.parent ! AggregateResult(recordCount, timeTaken, result.isTerminatedEarly, result.isTimedOut, res)
  }

  private def reshape(response: WrappedResponse) = {

    def reshape(result: Aggregations, aggSpecs: Seq[AggSpec]): JObject = {
      if(aggSpecs.nonEmpty) {
        val agg = aggSpecs.head
        var res: Aggregation = result.get(agg.name)
        while(res.isInstanceOf[Nested]||res.isInstanceOf[ReverseNested]) {
          res match {
            case nested1: Nested => res = nested1.getAggregations.get(agg.name)
            case _ => res = res.asInstanceOf[ReverseNested].getAggregations.get(agg.name)
          }
        }

        val currAgg = res.asInstanceOf[Terms]

        //error(currAgg.toString)
        val buckets = currAgg.getBuckets.drop(agg.offset)
        JObject(
          JField(agg.name,
            JObject(
              JField("group-count", JInt(buckets.size)),
              //JField("total-group-count", JInt(cardinalities(0))),
              JField("buckets",
                JObject(buckets.map( x=>
                  JField(x.getKeyAsString,
                    if(aggSpecs.size>1)
                      JObject(
                        JField("count", JInt(x.getDocCount)),
                        JField("sub", reshape(x.getAggregations, aggSpecs.drop(1)))
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

    //val cardinalities = response.aggParams.agg.aggSpecs.map(s=>response.result.getAggregations.get(s.name+"Count").asInstanceOf[Cardinality].getValue)
    reshape(response.result.getAggregations, response.aggParams.agg.aggSpecs)

  }
}

