package com.askme.mandelbrot.handler.search.geo

import java.io.Serializable
import java.util

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.message.ErrorResponse
import com.askme.mandelbrot.handler.search.geo.message.SearchResult
import com.askme.mandelbrot.handler.search.geo.message.GeoSearchParams
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
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder
import org.elasticsearch.search.sort.SortBuilders._
import org.elasticsearch.search.sort._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization


import scala.collection.JavaConversions._


/**
 * Created by adichad on 08/01/15.
 */
object GeoSearchRequestHandler extends Logging {

  val pat = """(?U)[^\p{alnum}]+"""

  private val randomParams = new util.HashMap[String, AnyRef]
  randomParams.put("buckets", int2Integer(5))

  private def getSort(lat: Double, lon: Double, w: Array[String], channel: String): List[SortBuilder] = {

    implicit val formats = org.json4s.DefaultFormats
    val sorters: List[SortBuilder] =
      if(lat==0.0d && lon==0.0d)
        (
          (if(channel == "grocery") {
            val serviceableParams = new util.HashMap[String, AnyRef]()
            serviceableParams.put("type", "String")
            serviceableParams.put("field", "tags")
            serviceableParams.put("mapper", Serialization.write(Map("grocery"->long2Long(0l))))
            serviceableParams.put("missing", long2Long(1l))

            Some(scriptSort(new Script("ordinal", ScriptType.INLINE, "native", serviceableParams), "number").order(SortOrder.ASC))
          } else None)::
          Some(scoreSort.order(SortOrder.DESC))::Nil
          ).flatten
      else if(w.isEmpty) (
        (if(channel == "grocery") {
          val serviceableParams = new util.HashMap[String, AnyRef]()
          serviceableParams.put("type", "String")
          serviceableParams.put("field", "tags")
          serviceableParams.put("mapper", Serialization.write(Map("grocery"->long2Long(0l))))
          serviceableParams.put("missing", long2Long(1l))

          Some(scriptSort(new Script("ordinal", ScriptType.INLINE, "native", serviceableParams), "number").order(SortOrder.ASC))
        } else None)::
        Some(geoDistanceSort("center").point(lat, lon).geoDistance(GeoDistance.PLANE).order(SortOrder.ASC).coerce(true))::Nil
        ).flatten
      else {
        val geoParams = new util.HashMap[String, AnyRef]
        geoParams.put("lat", double2Double(lat))
        geoParams.put("lon", double2Double(lon))
        geoParams.put("coordfield", "center")
        (
          (if(channel == "grocery") {
            val serviceableParams = new util.HashMap[String, AnyRef]()
            serviceableParams.put("type", "String")
            serviceableParams.put("field", "tags")
            serviceableParams.put("mapper", Serialization.write(Map("grocery"->long2Long(0l))))
            serviceableParams.put("missing", long2Long(1l))

            Some(scriptSort(new Script("ordinal", ScriptType.INLINE, "native", serviceableParams), "number").order(SortOrder.ASC))
          } else None)::
          Some(scriptSort(new Script("geodistancebucket", ScriptType.INLINE, "native", geoParams), "number").order(SortOrder.ASC))::
          Some(scoreSort.order(SortOrder.DESC))::Nil
        ).flatten
      }

    debug(sorters.toString)
    sorters

  }


  private def nestIfNeeded(fieldName: String, q: QueryBuilder): QueryBuilder = {
    /*val parts = fieldName.split("""\.""")
    if (parts.length > 1)
      nestedQuery(parts(0), q).scoreMode("max")
    else
    */
    q
  }



  implicit class DisMaxQueryPimp(val q: DisMaxQueryBuilder) {
    def addAll(queries: Iterable[QueryBuilder]) = {
      queries.foreach(q.add)
      q
    }
  }

  implicit class BoolQueryPimp(val q: BoolQueryBuilder) {
    def shouldAll(queries: Iterable[QueryBuilder]) = {
      queries.foreach(q.should)
      q
    }

    def shouldAll(queries: QueryBuilder*) = {
      queries.foreach(q.should)
      q
    }

    def mustAll(queries: Iterable[QueryBuilder]) = {
      queries.foreach(q.must)
      q
    }

    def mustAll(queries: QueryBuilder*) = {
      queries.foreach(q.must)
      q
    }

    def mustNotAll(queries: Iterable[QueryBuilder]) = {
      queries.foreach(q.mustNot)
      q
    }

    def mustNotAll(queries: QueryBuilder*) = {
      queries.foreach(q.mustNot)
      q
    }
  }

  implicit class SearchPimp(val search: SearchRequestBuilder) {
    def addSorts(sorts: Iterable[SortBuilder]) = {
      sorts.foreach(search.addSort)
      search
    }

    def addAggregations(aggregations: Iterable[AbstractAggregationBuilder]) = {
      aggregations.foreach(search.addAggregation)
      search
    }
  }

  implicit class AggregationPimp(val agg: TopHitsBuilder) {
    def addSorts(sorts: Iterable[SortBuilder]) = {
      sorts.foreach(agg.addSort)
      agg
    }
  }

  private def analyze(esClient: Client, index: String, field: String, text: String): Array[String] =
    new AnalyzeRequestBuilder(esClient.admin.indices, AnalyzeAction.INSTANCE, index, text).setField(field).get().getTokens.map(_.getTerm).toArray


  private val emptyStringArray = new Array[String](0)

  private def superBoost(len: Int) = math.pow(10, math.min(10,len+1)).toFloat

  private case class WrappedResponse(searchParams: GeoSearchParams, result: SearchResponse, query: QueryBuilder, relaxLevel: Int)


  private def buildQuery(w: Array[String], kw: String, esClient: Client, index: String) = {
    def shingleSpan(field: String, boost: Float, w: Array[String], fuzzyprefix: Int, maxShingle: Int, minShingle: Int = 1, sloppy: Boolean = true, fuzzy: Boolean = true) = {
      val fieldQuery1 = boolQuery.minimumShouldMatch("33%")
      val terms: Array[SpanQueryBuilder] = w.map(x=>
        if(x.length > 5 && fuzzy)
          spanMultiTermQueryBuilder(
            fuzzyQuery(field, x).prefixLength(fuzzyprefix).fuzziness(if(x.length > 9) Fuzziness.TWO else Fuzziness.ONE))
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

    def currQuery(tokenFields: Map[String, Float],
                          w: Array[String], fuzzy: Boolean = false, sloppy: Boolean = false, tokenRelax: Int = 0) = {

      disMaxQuery.addAll(tokenFields.map(field => shingleSpan(field._1, field._2, w, 1, w.length, math.max(w.length-tokenRelax, 1), sloppy, fuzzy)))
    }

    def shinglePartition(tokenFields: Map[String, Float], w: Array[String],
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

    def fuzzyOrTermQuery(field: String, word: String, exactBoost: Float, fuzzyPrefix: Int, fuzzy: Boolean = true) = {
      if(word.length > 5 && fuzzy)
        fuzzyQuery(field, word).prefixLength(fuzzyPrefix)
          .fuzziness(if(word.length > 9) Fuzziness.TWO else Fuzziness.ONE)
          .boost(if(word.length > 9) exactBoost/3f else exactBoost/2f)
      else
        termQuery(field, word).boost(exactBoost)

    }


    val w = analyze(esClient, index, "synonyms", kw)
    val query =
      if(kw.endsWith(" ")) {
        if (w.nonEmpty) {
          disMaxQuery.add(
            shinglePartition(
              Map("synonyms" -> 1e9f, "name" -> 1e11f,
                "containers_dag_name" -> 1e6f, "containers_dag_synonyms" -> 1e6f),
              w, w.length, 1, fuzzy = false, sloppy = false, tokenRelax = 0
            )
          ).add(
            shinglePartition(
              Map("synonyms" -> 1f, "name" -> 1e3f,
                "containers_dag_name" -> 1f, "containers_dag_synonyms" -> 1f),
              w, w.length, 1, fuzzy = true, sloppy = true, tokenRelax = 0
            )
          ).add(
            shinglePartition(
              Map("synonyms.token_edge_ngram" -> 1e1f, "name.token_edge_ngram" -> 1e2f,
                "containers_dag_name.token_edge_ngram" -> 1f, "containers_dag_synonyms.token_edge_ngram" -> 1f),
              w, w.length, 1, fuzzy = false, sloppy = true, tokenRelax = 0
            )
          ).add(
            shinglePartition(
              Map("synonyms.shingle_nospace_edge_ngram" -> 1e0f, "name.shingle_nospace_edge_ngram" -> 1e1f,
                "containers_dag_name.shingle_nospace_edge_ngram" -> 1f, "containers_dag_synonyms.shingle_nospace_edge_ngram" -> 1f),
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
              Map("synonyms" -> 1e9f, "name" -> 1e11f,
                "containers_dag_name" -> 1e6f, "containers_dag_synonyms" -> 1e6f),
              w, w.length, 1, fuzzy = false, sloppy = false, tokenRelax = 0
            )
          ).add(
            shinglePartition(
              Map("synonyms" -> 1e3f, "name" -> 1e5f,
                "containers_dag_name" -> 1f, "containers_dag_synonyms" -> 1f),
              w, w.length, 1, fuzzy = true, sloppy = true, tokenRelax = 0
            )
          ).add(
            shinglePartition(
              Map("synonyms.token_edge_ngram" -> 1e3f, "name.token_edge_ngram" -> 1e5f,
                "containers_dag_name.token_edge_ngram" -> 1f, "containers_dag_synonyms.token_edge_ngram" -> 1f),
              w, w.length, 1, fuzzy = false, sloppy = true, tokenRelax = 0
            )
          ).add(
            shinglePartition(
              Map("synonyms.shingle_nospace_edge_ngram" -> 1e1f, "name.shingle_nospace_edge_ngram" -> 1e2f,
                "containers_dag_name.shingle_nospace_edge_ngram" -> 1f, "containers_dag_synonyms.shingle_nospace_edge_ngram" -> 1f),
              w, w.length, 1, fuzzy = false, sloppy = true, tokenRelax = 0
            )
          )

          val q2 = boolQuery
          if (front.nonEmpty) {
            q2.must(
              disMaxQuery.add(
                shinglePartition(
                  Map("synonyms" -> 1e9f, "name" -> 1e11f,
                    "containers_dag_name" -> 1e6f, "containers_dag_synonyms" -> 1e6f),
                  front, front.length, 1, fuzzy = false, sloppy = false, tokenRelax = 0
                )
              ).add(
                shinglePartition(
                  Map("synonyms" -> 1e3f, "name" -> 1e5f,
                    "containers_dag_name" -> 1f, "containers_dag_synonyms" -> 1f),
                  front, front.length, 1, fuzzy = true, sloppy = true, tokenRelax = 0
                )
              ).add(
                shinglePartition(
                  Map("synonyms.token_edge_ngram" -> 1e3f, "name.token_edge_ngram" -> 1e5f,
                    "containers_dag_name.token_edge_ngram" -> 1f, "containers_dag_synonyms.token_edge_ngram" -> 1f),
                  front, front.length, 1, fuzzy = false, sloppy = true, tokenRelax = 0
                )
              ).add(
                shinglePartition(
                  Map("synonyms.shingle_nospace_edge_ngram" -> 1e1f, "name.shingle_nospace_edge_ngram" -> 1e2f,
                    "containers_dag_name.shingle_nospace_edge_ngram" -> 1f, "containers_dag_synonyms.shingle_nospace_edge_ngram" -> 1f),
                  front, front.length, 1, fuzzy = false, sloppy = true, tokenRelax = 0
                )
              )
            )
          }

          val q3 = disMaxQuery
            .add(fuzzyOrTermQuery("name.token_edge_ngram", last_raw, 1e19f, 1, fuzzy = true))
            .add(fuzzyOrTermQuery("name.shingle_nospace_edge_ngram", last_raw, 1e17f, 1, fuzzy = true))
          if(front.isEmpty)
            q3.add(fuzzyOrTermQuery("name.keyword_edge_ngram", last_raw, 1e21f, 1, fuzzy = true))

          q3.add(fuzzyOrTermQuery("synonyms.token_edge_ngram", last_raw, 1e17f, 1, fuzzy = false))
            .add(fuzzyOrTermQuery("synonyms.shingle_nospace_edge_ngram", last_raw, 1e15f, 1, fuzzy = false))
          if (front.isEmpty)
            q3.add(fuzzyOrTermQuery("synonyms.keyword_edge_ngram", last_raw, 1e19f, 1, fuzzy = false))

          q3.add(fuzzyOrTermQuery("containers_dag_name.token_edge_ngram", last_raw, 1e10f, 1, fuzzy = false))
            .add(fuzzyOrTermQuery("containers_dag_name.shingle_nospace_edge_ngram", last_raw, 1e8f, 1, fuzzy = false))
          if (front.isEmpty)
            q3.add(fuzzyOrTermQuery("containers_dag_name.keyword_edge_ngram", last_raw, 1e12f, 1, fuzzy = false))

          q3.add(fuzzyOrTermQuery("containers_dag_synonyms.token_edge_ngram", last_raw, 1e10f, 1, fuzzy = false))
            .add(fuzzyOrTermQuery("containers_dag_synonyms.shingle_nospace_edge_ngram", last_raw, 1e8f, 1, fuzzy = false))
          if (front.isEmpty)
            q3.add(fuzzyOrTermQuery("containers_dag_synonyms.keyword_edge_ngram", last_raw, 1e12f, 1, fuzzy = false))

          q.add(if (q2.hasClauses) q2.must(q3) else q3)
        } else
          matchAllQuery
      }
    query
  }

}


class GeoSearchRequestHandler(val config: Config, serverContext: SearchContext) extends Actor with Configurable with Logging {
  import GeoSearchRequestHandler._

  private val esClient: Client = serverContext.esClient
  private var w = emptyStringArray

  private def buildFilter(searchParams: GeoSearchParams, externalFilter: JValue): BoolQueryBuilder = {
    import searchParams.filters._
    import searchParams.idx._
    import searchParams._
    import searchParams.geo._
    import searchParams.text._

    implicit val formats = org.json4s.DefaultFormats

    // filters
    val finalFilter = boolQuery()
    if(externalFilter!=JNothing)
      finalFilter.must(QueryBuilders.wrapperQuery(compact(externalFilter)))

    finalFilter.must(termQuery("archived", false))
    if (gids.trim != "") {
      finalFilter.must(termsQuery("gid", gids.split(",").map(_.trim.toLong):_*))
    }
    if (`type`.trim != "") {
      finalFilter.must(termQuery("types", `type`.trim))
    }
    if (tags.trim != "") {
      finalFilter.must(termsQuery("tags.exact", tags.split(",").map(tag=>analyze(esClient, index, "tags.exact", tag).mkString(" ")):_*))
    } else if(channel == "grocery" && kw == "") {
      finalFilter.must(termsQuery("tags.exact", tags.split(",").map(tag=>analyze(esClient, index, "tags.exact", tag).mkString(" ")):_*))
    }
    if (phone_prefix.trim != "") {
      finalFilter.must(termQuery("phone_prefix.exact", analyze(esClient, index, "phone_prefix.exact", phone_prefix).mkString(" ")))
    }

    val containerFilter = boolQuery()
    if (container.gids.trim != "") {
      containerFilter.must(termsQuery("containers_dag.gid", container.gids.split(",").map(_.trim.toLong):_*))
    }
    if (container.`type`.trim != "") {
      containerFilter.must(termQuery("containers_dag.types", container.`type`.trim))
    }
    if (container.tags.trim != "") {
      containerFilter.must(termsQuery("containers_dag.tags", container.tags.split(",").map(tag=>analyze(esClient, index, "tags.exact", tag).mkString(" ")):_*))
    }
    if (container.phone_prefix.trim != "") {
      containerFilter.must(termQuery("containers_dag.phone_prefix.exact", analyze(esClient, index, "phone_prefix.exact", container.phone_prefix).mkString(" ")))
    }
    if (container.text.trim != "") {
      containerFilter.must(
        boolQuery()
          .should(termQuery("containers_dag.name.exact", analyze(esClient, index, "name.exact", container.text).mkString(" ")))
          .should(termQuery("containers_dag.synonyms.exact", analyze(esClient, index, "synonyms.exact", container.text).mkString(" ")))
      )
    }

    if(containerFilter.hasClauses)
      finalFilter.must(nestedQuery("containers_dag", containerFilter))


    val relatedFilter = boolQuery()
    if (related.gids.trim != "") {
      relatedFilter.must(termsQuery("related_list.gid", related.gids.split(",").map(_.trim.toLong):_*))
    }
    if (related.`type`.trim != "") {
      relatedFilter.must(termQuery("related_list.types", related.`type`.trim))
    }
    if (related.tags.trim != "") {
      relatedFilter.must(termsQuery("related_list.tags", related.tags.split(",").map(tag=>analyze(esClient, index, "tags.exact", tag).mkString(" ")):_*))
    }
    if (related.phone_prefix.trim != "") {
      relatedFilter.must(termQuery("related_list.phone_prefix.exact", analyze(esClient, index, "phone_prefix.exact", related.phone_prefix).mkString(" ")))
    }
    if (related.text.trim != "") {
      relatedFilter.must(
        boolQuery()
          .should(termQuery("related_list.name.exact", analyze(esClient, index, "name.exact", related.text).mkString(" ")))
          .should(termQuery("related_list.synonyms.exact", analyze(esClient, index, "synonyms.exact", related.text).mkString(" ")))
      )
    }

    if(relatedFilter.hasClauses)
      finalFilter.must(nestedQuery("related_list", relatedFilter))

    if ((lat != 0.0d || lon != 0.0d) && tokm > 0.0d) {
      finalFilter
        .must(geoHashCellQuery("center").point(lat, lon).precision(tokm.toInt+"km").neighbors(true))
    }

    finalFilter
  }

  private def buildSearch(searchParams: GeoSearchParams): SearchRequestBuilder = {
    import searchParams.idx._
    import searchParams.limits._
    import searchParams.page._
    import searchParams.view._
    import searchParams.geo._
    import searchParams.filters._

    val sorters = getSort(lat, lon, w, channel)

    val search: SearchRequestBuilder = esClient.prepareSearch(index.split(","): _*)
      .setTypes(esType.split(","): _*)
      .setTrackScores(true)
      .setTimeout(TimeValue.timeValueMillis(Math.min(timeoutms, long("timeoutms"))))
      .setTerminateAfter(Math.min(maxdocspershard, int("max-docs-per-shard")))
      .setExplain(explain)
      .setSearchType(SearchType.fromString(searchType, ParseFieldMatcher.STRICT))
      .addSorts(sorters)
      .setFrom(offset).setSize(size)
      .setFetchSource(select.split(""",""").map(_.trim), Array[String]())

    search
  }


  override def receive = {
    case searchParams: GeoSearchParams =>
      import searchParams.idx._
      import searchParams.req._
      import searchParams.startTime
      import searchParams.text._

      try {
        val externalString = httpReq.entity.data.asString
        val externals = parseOpt(externalString).getOrElse(JNothing)

        w = analyze(esClient, index, "name", kw)
        if (w.length>20) w = emptyStringArray
        w = w.take(5)

        val query = buildQuery(w, kw, esClient, index)

        // filters
        val finalFilter = buildFilter(searchParams, externals\"filter")

        val search = buildSearch(searchParams)

        val qfinal = boolQuery.must(query).filter(finalFilter)
        search.setQuery(qfinal)

        val me = context.self

        search.execute(new ActionListener[SearchResponse] {
          override def onResponse(response: SearchResponse): Unit = {
            me ! WrappedResponse(searchParams, response, qfinal, 0)
          }

          override def onFailure(e: Throwable): Unit = {
            val reqBodyRaw = httpReq.entity.data.asString
            val temp = parseOpt(reqBodyRaw).getOrElse(JNothing)
            val reqBody = if(temp!=JNothing)compact(temp) else reqBodyRaw
            val timeTaken = System.currentTimeMillis() - startTime
            error("[" + timeTaken + "] [q0] [na/na] [" + clip.toString + "]->[" + httpReq.uri + " -d "+reqBody+"]->[]", e)
            context.parent ! ErrorResponse(e.getMessage, e)
          }
        })
      } catch {
        case e: Throwable =>
          val reqBodyRaw = httpReq.entity.data.asString
          val temp = parseOpt(reqBodyRaw).getOrElse(JNothing)
          val reqBody = if(temp!=JNothing)compact(temp) else reqBodyRaw
          val timeTaken = System.currentTimeMillis() - startTime
          error("[" + timeTaken + "] [q0] [na/na] [" + clip.toString + "]->[" + httpReq.uri + " -d "+reqBody+"]->[]", e)
          context.parent ! ErrorResponse(e.getMessage, e)
      }

    case response: WrappedResponse =>
      import response.searchParams.limits._
        import response.searchParams.req._
        import response.searchParams.startTime
        import response.searchParams.view._
        import response.{relaxLevel, result, _}
      try {
        val parsedResult = parse(result.toString)

        val reqBodyRaw = httpReq.entity.data.asString
        val temp = parseOpt(reqBodyRaw).getOrElse(JNothing)
        val reqBody = if(temp!=JNothing)compact(temp) else reqBodyRaw
        val timeTaken = System.currentTimeMillis - startTime

        info("[" + result.getTookInMillis + "/" + timeTaken + (if (result.isTimedOut) " timeout" else "") + "] [q" + relaxLevel + "] [" + result.getHits.hits.length + "/" + result.getHits.getTotalHits + (if (result.isTerminatedEarly) " termearly (" + Math.min(maxdocspershard, int("max-docs-per-shard")) + ")" else "") + "] [" + clip.toString + "]->[" + httpReq.uri + " -d "+reqBody+"]")
        context.parent ! SearchResult(result.getHits.hits.length, timeTaken, relaxLevel, if(explain) parse(query.toString) else JObject(), parsedResult)
      } catch {
        case e: Throwable =>
          val reqBodyRaw = httpReq.entity.data.asString
          val temp = parseOpt(reqBodyRaw).getOrElse(JNothing)
          val reqBody = if(temp!=JNothing)compact(temp) else reqBodyRaw
          val timeTaken = System.currentTimeMillis() - startTime
          error("[" + timeTaken + "] [q" + relaxLevel + "] [na/na] [" + clip.toString + "]->[" + httpReq.uri + " -d "+reqBody+ "]->[]", e)
          context.parent ! ErrorResponse(e.getMessage, e)
      }
  }
}

