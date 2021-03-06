package com.askme.mandelbrot.handler.search.grocery

import java.util

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.message.ErrorResponse
import com.askme.mandelbrot.handler.search.grocery.message.GrocerySearchParams
import com.askme.mandelbrot.handler.search.grocery.message.SearchResult
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
import org.elasticsearch.index.query.support.QueryInnerHitBuilder
import org.elasticsearch.search.aggregations.{AbstractAggregationBuilder, Aggregator}
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.{SignificanceHeuristic, SignificanceHeuristicBuilder}
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder
import org.elasticsearch.search.fetch.innerhits.InnerHitsBuilder
import org.elasticsearch.search.sort.SortBuilders._
import org.elasticsearch.search.sort._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConversions._


/**
 * Created by adichad on 08/01/15.
 */
object GrocerySearchRequestHandler extends Logging {

  val pat = """(?U)[^\p{alnum}]+"""

  private val randomParams = new util.HashMap[String, AnyRef]
  randomParams.put("buckets", int2Integer(5))

  private def getSort(sort: String, w: Array[String], categories: Array[String], brands: Array[String],
                      itemFilter: QueryBuilder, pla: Boolean): List[SortBuilder] = {

    val sorters =
    if(sort=="price.asc") {
      List(fieldSort("items.customer_price").setNestedPath("items").order(SortOrder.ASC).sortMode("min")
        .setNestedFilter(itemFilter))
    }
    else if(sort=="price.desc") {
      List(fieldSort("items.customer_price").setNestedPath("items").order(SortOrder.DESC).sortMode("min")
        .setNestedFilter(itemFilter))
    }
    else if(sort=="alpha.asc") {
      List(fieldSort("variant_title.agg").order(SortOrder.ASC),
        fieldSort("items.customer_price").setNestedPath("items").order(SortOrder.ASC).sortMode("min")
          .setNestedFilter(itemFilter))
    }
    else if(sort=="alpha.desc") {
      List(fieldSort("variant_title.agg").order(SortOrder.DESC),
        fieldSort("items.customer_price").setNestedPath("items").order(SortOrder.ASC).sortMode("min")
          .setNestedFilter(itemFilter))
    }
    else {
      val b = boolQuery
      categories.foreach { cat =>
        b.should(termQuery("category_hierarchy.name.exact", cat))
      }
      (
        Some(scoreSort().order(SortOrder.DESC)) ::
          (
            if(b.hasClauses)
              Some(
                fieldSort("category_hierarchy.category_brand_priority").setNestedPath("category_hierarchy")
                  .order(SortOrder.DESC).sortMode("max").setNestedFilter(b)
              )
            else
              None
            )::
          (
            if(b.hasClauses)
              Some(
                fieldSort("category_hierarchy.category_priority").setNestedPath("category_hierarchy")
                  .order(SortOrder.DESC).sortMode("max").setNestedFilter(b)
              )
            else
              Some(
                fieldSort("category_hierarchy.category_priority").setNestedPath("category_hierarchy")
                  .order(SortOrder.DESC).sortMode("max")
              )
            )::
          Some(fieldSort("items.gsv").setNestedPath("items").order(SortOrder.DESC).sortMode("max")
            .setNestedFilter(itemFilter)) ::
          Some(fieldSort("items.customer_price").setNestedPath("items").order(SortOrder.ASC).sortMode("min")
            .setNestedFilter(itemFilter)) ::
          Some(fieldSort("variant_id").order(SortOrder.DESC)) ::
          Nil
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

  private def shingleSpan(field: String, boost: Float, w: Array[String], fuzzyprefix: Int, maxShingle: Int, minShingle: Int = 1, sloppy: Boolean = true, fuzzy: Boolean = true) = {
    val fieldQuery1 = boolQuery.minimumShouldMatch("33%")
    val terms: Array[SpanQueryBuilder] = w.map(x=>
      if(x.length > 4 && fuzzy)
        spanMultiTermQueryBuilder(
          fuzzyQuery(field, x).prefixLength(fuzzyprefix).fuzziness(if(x.length > 7) Fuzziness.TWO else Fuzziness.ONE))
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
  val forceSpan = Map("variant_title.exact"->"variant_title", "variant_title_head.exact"->"variant_title_head", "brand_name.exact"->"brand_name",
    "categories.name.exact"->"categories.name")

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


  def currQuerySuggest(tokenFields: Map[String, Float],
                       w: Array[String], fuzzy: Boolean = false, sloppy: Boolean = false, tokenRelax: Int = 0) = {

    disMaxQuery.addAll(tokenFields.map(field => shingleSpan(field._1, field._2, w, 1, w.length, math.max(w.length-tokenRelax, 1), sloppy, fuzzy)))
  }

  def shinglePartitionSuggest(tokenFields: Map[String, Float], w: Array[String],
                              maxShingle: Int, minShingle: Int = 1, fuzzy: Boolean = false, sloppy: Boolean = false,
                              tokenRelax: Int = 0): BoolQueryBuilder = {

    if(w.length>0)
      boolQuery.minimumNumberShouldMatch(1).shouldAll(
        (math.max(1, math.min(minShingle, w.length)) to math.min(maxShingle, w.length)).map(len=>(w.slice(0, len), w.slice(len, w.length))).map { x =>
          if (x._2.length > 0)
            shinglePartitionSuggest(tokenFields, x._2, maxShingle, minShingle, fuzzy, sloppy, tokenRelax)
              .must(currQuerySuggest(tokenFields, x._1, fuzzy, sloppy, tokenRelax))
          else
            currQuerySuggest(tokenFields, x._1, fuzzy, sloppy, tokenRelax)
        }
      )
    else
      boolQuery
  }

  private def fuzzyOrTermQuery(field: String, word: String, exactBoost: Float, fuzzyPrefix: Int, fuzzy: Boolean = true) = {
      if(word.length > 4 && fuzzy)
        fuzzyQuery(field, word).prefixLength(fuzzyPrefix)
          .fuzziness(if(word.length > 7) Fuzziness.TWO else Fuzziness.ONE)
          .boost(if(word.length > 7) exactBoost/3f else exactBoost/2f)
      else
        termQuery(field, word).boost(exactBoost)

  }

  private def analyze(esClient: Client, index: String, field: String, text: String): Array[String] =
    new AnalyzeRequestBuilder(esClient.admin.indices, AnalyzeAction.INSTANCE, index, text).setField(field).get().getTokens.map(_.getTerm).toArray


  private val searchFields2 = Map(
    "variant_title_head" -> 1e12f,
    "variant_title" -> 1e8f,
    "brand_name" -> 1e2f,
    "categories.name" -> 1e5f)

  private val fullFields2 = Map(
    "variant_title_head.exact" -> 1e12f,
    "variant_title.exact"->1e10f,
    "brand_name.exact" -> 1e2f,
    "categories.name.exact"->1e6f)


  private val emptyStringArray = new Array[String](0)

  private def superBoost(len: Int) = math.pow(10, math.min(10,len+1)).toFloat

  private case class WrappedResponse(searchParams: GrocerySearchParams, result: SearchResponse, query: QueryBuilder, relaxLevel: Int)
  private case class ReSearch(searchParams: GrocerySearchParams, filter: BoolQueryBuilder, search: SearchRequestBuilder, query: QueryBuilder, relaxLevel: Int, response: SearchResponse)

  private def queryBuilder(tokenFields: Map[String, Float], recomFields: Map[String, Float], fuzzy: Boolean = false, sloppy: Boolean = false, span: Boolean = false, minShingle: Int = 1, tokenRelax: Int = 0)
                          (w: Array[String], maxShingle: Int) = {
    shinglePartition(tokenFields, recomFields, w, maxShingle, minShingle, fuzzy, sloppy, span, tokenRelax)
  }

  private val qDefs: Seq[((Array[String], Int)=>QueryBuilder, Int)] = Seq(

    (queryBuilder(searchFields2, fullFields2, fuzzy = false, sloppy = true, span = false, 1, 0), 1), //1
    // full-shingle exact full matches

    //(queryBuilder(searchFields2, fullFields2, fuzzy = true, sloppy = true, span = false, 1, 0), 1), //3
    // full-shingle fuzzy full matches

    //(queryBuilder(searchFields2, fullFields2, fuzzy = false, sloppy = true, span = true, 1, 0), 1), //5
    // full-shingle exact sloppy-span matches

    //(queryBuilder(searchFields2, fullFields2, fuzzy = true, sloppy = false, span = false, 1, 1), 1), //6
    // relaxed-shingle fuzzy full matches

    (queryBuilder(searchFields2, fullFields2, fuzzy = true, sloppy = true, span = true, 2, 1), 1)//7
    // relaxed-shingle fuzzy sloppy-span matches

  )

}



class GrocerySearchRequestHandler(val parentPath: String, serverContext: SearchContext) extends Actor with Configurable with Logging {
  import GrocerySearchRequestHandler._

  private val esClient: Client = serverContext.esClient
  private var w = emptyStringArray
  private var orderFilter: BoolQueryBuilder = null
  private var itemFilter: QueryBuilder = null


  private def buildFilter(searchParams: GrocerySearchParams, externalFilter: JValue): BoolQueryBuilder = {
    import searchParams.filters._
    import searchParams.idx._
    import searchParams.view._
    implicit val formats = org.json4s.DefaultFormats

    // filters
    val finalFilter = boolQuery()
    finalFilter
      .must(termQuery("variant_status", 0))
      .must(termQuery("product_status", 0))
      .must(termQuery("categories.status", 1))
      .must(termQuery("brand_status", 1))
    if(storefront_id ==0 )
      finalFilter.must(termQuery("categories.status", 1))

    finalFilter.must(termQuery("categories.parent_name", "fmcg"))

    val itemFilter = boolQuery()
    if(!include_inactive_items)
      itemFilter.must(termQuery("items.status", 1))
    if(!include_inactive_items)
      itemFilter.must(termQuery("items.login_status", 1))

    if (item_id != "") {
      itemFilter.must(termsQuery("items.id", item_id.split(""",""").map(_.toInt):_*))
    }
    val zoneFilter = boolQuery

    zone_code.split( """\|""").map(_.trim).filter(_.nonEmpty).foreach { z =>
      zoneFilter.should(boolQuery.must(termQuery("items.zone_code", z)).must(termQuery("items.zone_status",0)))
    }

    if(zoneFilter.hasClauses)
      itemFilter.must(zoneFilter)

    if(geo_id != 0) {
      itemFilter.must(termQuery("items.areas", geo_id))
    }

    if(pla) {
      itemFilter.must(termQuery("items.pla_status", 1))
    }

    if(city_id>0) {
      itemFilter.must(termQuery("items.city_id", city_id))
    }

    if(storefront_id != 0) {
      itemFilter.must(nestedQuery("items.storefronts", termQuery("items.storefronts.id", storefront_id)))
    }

    val orderFilter = boolQuery()
    if(order_user_id!="")
      orderFilter.must(termQuery("items.orders.user_id", order_user_id))

    if(order_id!="")
      orderFilter.must(termQuery("items.orders.order_id", order_id))

    if(parent_order_id!="")
      orderFilter.must(termQuery("items.orders.parent_order_id", parent_order_id))

    if(order_status!="")
      orderFilter.must(termQuery("items.orders.status_code", order_status.trim))

    if(order_updated_since!="")
      orderFilter.must(rangeQuery("items.orders.updated_on").format("yyyy-MM-dd").from(order_updated_since))

    if(order_geo_id>0l)
      orderFilter.must(termQuery("items.orders.geo_id", order_geo_id))

    if(orderFilter.hasClauses) {
      itemFilter.must(nestedQuery("items.orders", orderFilter))
      this.orderFilter = orderFilter
    }

    this.itemFilter = QueryBuilders.wrapperQuery(itemFilter.buildAsBytes())

    finalFilter
      .must(
        nestedQuery("items",itemFilter)
          .innerHit(
            new QueryInnerHitBuilder().setName("matched_items")
              .addSort(if(pla) "items.pla_customer_price" else "items.customer_price", SortOrder.ASC)
              .addSort(if(pla) "items.pla_status" else "items.deal_count", SortOrder.DESC)
              .addSort(if(pla) "items.pla_margin" else "items.margin", SortOrder.DESC)
              .setFrom(0).setSize(1)
              .setFetchSource(Array("*"), Array[String]())
              .setExplain(explain)))
    if(externalFilter!=JNothing)
      finalFilter.must(QueryBuilders.wrapperQuery(compact(externalFilter)))

    if (variant_id != 0) {
      finalFilter.must(termQuery("variant_id", variant_id))
    }
    if (product_id != 0) {
      finalFilter.must(termQuery("product_id", product_id))
    }

    if (category != "") {
      val b = boolQuery
      category.split("""\|""").map(analyze(esClient, index, "categories.name.exact", _).mkString(" ")).filter(!_.isEmpty).foreach { cat =>
        b.should(termQuery("category_hierarchy.name.exact", cat))
      }
      if(b.hasClauses)
        finalFilter.must(nestedQuery("category_hierarchy", b))
    }

    if (brand != "") {
      val b = boolQuery
      brand.split("""\|""").map(analyze(esClient, index, "brand_name.exact", _).mkString(" ")).filter(!_.isEmpty).foreach { brand =>
        b.should(termQuery("brand_name.exact", brand))
      }
      if(b.hasClauses)
        finalFilter.must(b)
    }

    finalFilter
  }

  private def buildSearch(searchParams: GrocerySearchParams): SearchRequestBuilder = {
    import searchParams.idx._
    import searchParams.limits._
    import searchParams.page._
    import searchParams.view._
    import searchParams.filters._

    val sorters = getSort(sort, w,
      category.split("""\|""").map(analyze(esClient, index, "categories.name.exact", _).mkString(" ")).filter(!_.isEmpty),
      brand.split("""\|""").map(analyze(esClient, index, "brand_name.exact", _).mkString(" ")).filter(!_.isEmpty),
      itemFilter, pla
    )

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

    if(orderFilter!=null && orderFilter.hasClauses)
    search.addInnerHit("matched_orders",
      new InnerHitsBuilder.InnerHit()
        .setQuery(orderFilter)
        .setPath("items.orders")
        .setFetchSource("*", null)
        .setFrom(0).setSize(100)
    )


    if (agg) {
      if (category == ""||category.contains('|')) {
        val clevel = Math.min(Math.max(1, cat_level), 3)
        search.addAggregation(
          terms("categories").field(s"category_l${clevel.toString}.name.agg").size(aggbuckets).subAggregation(
            terms("id").field(s"category_l${clevel.toString}.id").size(1)
          )
        )
      }

      search.addAggregation(
        terms("categories_l1").field("category_l1.name.agg").size(aggbuckets).subAggregation(
          terms("id").field("category_l1.id").size(1)
        ).subAggregation(
          terms("categories_l2").field("category_l2.name.agg").size(aggbuckets).subAggregation(
            terms("id").field("category_l2.id").size(1)
          ).subAggregation(
            terms("categories_l3").field("category_l3.name.agg").size(aggbuckets).subAggregation(
              terms("id").field("category_l3.id").size(1)
            )
          )
        )
      )

      if (storefront_id == 0) {
        search.addAggregation(
          nested("items").path("items").subAggregation(
            nested("storefronts").path("items.storefronts").subAggregation(
              terms("ids").field("items.storefronts.id").size(aggbuckets)
            )
          ).subAggregation(
            nested("orders").path("items.orders").subAggregation(
              terms("status").field("items.orders.status_code").size(aggbuckets)
            )
          )
        )
      }

      if (brand == ""||brand.contains('|'))
        search.addAggregation(terms("brands").field("brand_name.agg").size(aggbuckets))
      
    }

    if(extended_agg) {
      search.addAggregation(
        nested("items_extended").path("items").subAggregation(
          nested("orders").path("items.orders").subAggregation(
            significantTerms("interesting sold items")
              .backgroundFilter(
                existsQuery("items.orders.item_set")
              )
              .field("items.orders.item_set").size(aggbuckets)
              .minDocCount(1)
          )
        )
      )
    }

    search
  }


  def buildSuggestQuery(kw: String, w: Array[String]) = {
    val query =
      if(kw.endsWith(" ")) {
        if (w.nonEmpty) {
          disMaxQuery.add(
            shinglePartitionSuggest(
              Map(
                "variant_title_head" -> 1e14f,
                "variant_title" -> 1e12f,
                "categories.name" -> 1e9f,
                "brand_name" -> 1e11f),
              w, w.length, 1, fuzzy = false, sloppy = false, tokenRelax = 0
            )
          ).add(
            shinglePartitionSuggest(
              Map(
                "variant_title_head" -> 1e8f,
                "variant_title" -> 1e6f,
                "categories.name" -> 1e4f,
                "brand_name" -> 1e5f),
              w, w.length, 1, fuzzy = true, sloppy = true, tokenRelax = 0
            )
          ).add(
            shinglePartitionSuggest(
              Map(
                "variant_title_head.token_edge_ngram" -> 1e4f,
                "variant_title.token_edge_ngram" -> 1e2f,
                "categories.name.token_edge_ngram" -> 1e1f,
                "brand_name.token_edge_ngram" -> 1e2f),
              w, w.length, 1, fuzzy = false, sloppy = true, tokenRelax = 0
            )
          ).add(
            shinglePartitionSuggest(
              Map(
                "variant_title_head.shingle_nospace_edge_ngram" -> 1e4f,
                "variant_title.shingle_nospace_edge_ngram" -> 1e2f,
                "categories.name.shingle_nospace_edge_ngram" -> 1e1f,
                "brand_name.shingle_nospace_edge_ngram" -> 1e2f),
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
            shinglePartitionSuggest(
              Map(
                "variant_title_head" -> 1e14f,
                "variant_title" -> 1e12f,
                "categories.name" -> 1e9f,
                "brand_name" -> 1e11f),
              w, w.length, 1, fuzzy = false, sloppy = false, tokenRelax = 0
            )
          ).add(
            shinglePartitionSuggest(
              Map(
                "variant_title_head" -> 1e8f,
                "variant_title" -> 1e6f,
                "categories.name" -> 1e3f,
                "brand_name" -> 1e5f),
              w, w.length, 1, fuzzy = true, sloppy = true, tokenRelax = 0
            )
          ).add(
            shinglePartitionSuggest(
              Map(
                "variant_title_head.token_edge_ngram" -> 1e4f,
                "variant_title.token_edge_ngram" -> 1e2f,
                "categories.name.token_edge_ngram" -> 1e1f,
                "brand_name.token_edge_ngram" -> 1e2f),
              w, w.length, 1, fuzzy = false, sloppy = true, tokenRelax = 0
            )
          ).add(
            shinglePartitionSuggest(
              Map(
                "variant_title_head.shingle_nospace_edge_ngram" -> 1e4f,
                "variant_title.shingle_nospace_edge_ngram" -> 1e2f,
                "categories.name.shingle_nospace_edge_ngram" -> 1e1f,
                "brand_name.shingle_nospace_edge_ngram" -> 1e2f),
              w, w.length, 1, fuzzy = false, sloppy = true, tokenRelax = 0
            )
          )

          val q2 = boolQuery
          if (front.nonEmpty) {
            q2.must(
              disMaxQuery.add(
                shinglePartitionSuggest(
                  Map(
                    "variant_title_head" -> 1e14f,
                    "variant_title" -> 1e12f,
                    "categories.name" -> 1e9f,
                    "brand_name" -> 1e11f),
                  front, front.length, 1, fuzzy = false, sloppy = false, tokenRelax = 0
                )
              ).add(
                shinglePartitionSuggest(
                  Map(
                    "variant_title_head" -> 1e8f,
                    "variant_title" -> 1e6f,
                    "categories.name" -> 1e3f,
                    "brand_name" -> 1e5f),
                  front, front.length, 1, fuzzy = true, sloppy = true, tokenRelax = 0
                )
              ).add(
                shinglePartitionSuggest(
                  Map(
                    "variant_title_head.token_edge_ngram" -> 1e4f,
                    "variant_title.token_edge_ngram" -> 1e2f,
                    "categories.name.token_edge_ngram" -> 1e1f,
                    "brand_name.token_edge_ngram" -> 1e2f),
                  front, front.length, 1, fuzzy = false, sloppy = true, tokenRelax = 0
                )
              ).add(
                shinglePartitionSuggest(
                  Map(
                    "variant_title_head.shingle_nospace_edge_ngram" -> 1e4f,
                    "variant_title.shingle_nospace_edge_ngram" -> 1e2f,
                    "categories.name.shingle_nospace_edge_ngram" -> 1e1f,
                    "brand_name.shingle_nospace_edge_ngram" -> 1e2f),
                  front, front.length, 1, fuzzy = false, sloppy = true, tokenRelax = 0
                )
              )
            )
          }

          val q3 = disMaxQuery
            .add(fuzzyOrTermQuery("variant_title_head.token_edge_ngram", last_raw, 1e20f, 1, fuzzy = true))
            .add(fuzzyOrTermQuery("variant_title_head.shingle_nospace_edge_ngram", last_raw, 1e20f, 1, fuzzy = false))

          q3.add(fuzzyOrTermQuery("variant_title.token_edge_ngram", last_raw, 1e19f, 1, fuzzy = true))
            .add(fuzzyOrTermQuery("variant_title.shingle_nospace_edge_ngram", last_raw, 1e17f, 1, fuzzy = false))

          q3.add(fuzzyOrTermQuery("categories.name.token_edge_ngram", last_raw, 1e17f, 1, fuzzy = false))
            .add(fuzzyOrTermQuery("categories.name.shingle_nospace_edge_ngram", last_raw, 1e15f, 1, fuzzy = false))

          q3.add(fuzzyOrTermQuery("brand_name.token_edge_ngram", last_raw, 1e17f, 1, fuzzy = false))
            .add(fuzzyOrTermQuery("brand_name.shingle_nospace_edge_ngram", last_raw, 1e15f, 1, fuzzy = false))


          q.add(if (q2.hasClauses) q2.must(q3) else q3)
        } else
          matchAllQuery
      }
    query
  }

  override def receive = {
      case searchParams: GrocerySearchParams =>
        import searchParams.filters._
        import searchParams.idx._
        import searchParams.req._
        import searchParams.startTime
        import searchParams.text._

        try {
          val externalString = httpReq.entity.data.asString
          val externals = parseOpt(externalString).getOrElse(JNothing)

          w = analyze(esClient, index, "variant_title", kw)
          if (w.length>20) w = emptyStringArray
          w = w.take(8)

          val query = if(suggest) buildSuggestQuery(kw, w) else {
            if (w.length > 0) qDefs.head._1(w, w.length)
            else matchAllQuery()
          }
          val leastCount = qDefs.head._2
          val isMatchAll = suggest || query.isInstanceOf[MatchAllQueryBuilder]

          // filters
          val finalFilter = buildFilter(searchParams, externals\"filter")

          val search = buildSearch(searchParams)

          val qfinal = boolQuery.must(query).filter(finalFilter)
          search.setQuery(qfinal)

          val me = context.self

          search.execute(new ActionListener[SearchResponse] {
            override def onResponse(response: SearchResponse): Unit = {
              if (response.getHits.totalHits() >= leastCount || isMatchAll)
                me ! WrappedResponse(searchParams, response, qfinal, 0)
              else
                me ! ReSearch(searchParams, finalFilter, search, qfinal, 1, response)
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

      case ReSearch(searchParams, filter, search, qfinal, relaxLevel, response) =>
        import searchParams.req._
        import searchParams.startTime

        try {

          if (relaxLevel >= qDefs.length)
            context.self ! WrappedResponse(searchParams, response, qfinal, relaxLevel - 1)
          else {
            val query = qDefs(relaxLevel)._1(w, w.length)
            val leastCount = qDefs(relaxLevel)._2
            val me = context.self

            val qfinal = boolQuery.must(query).filter(filter)
            search.setQuery(qfinal)
            search.execute(new ActionListener[SearchResponse] {
              override def onResponse(response: SearchResponse): Unit = {
                if (response.getHits.totalHits() >= leastCount || relaxLevel >= qDefs.length - 1)
                  me ! WrappedResponse(searchParams, response, qfinal, relaxLevel)
                else
                  me ! ReSearch(searchParams, filter, search, qfinal, relaxLevel + 1, response)
              }

              override def onFailure(e: Throwable): Unit = {
                val reqBodyRaw = httpReq.entity.data.asString
                val temp = parseOpt(reqBodyRaw).getOrElse(JNothing)
                val reqBody = if(temp!=JNothing)compact(temp) else reqBodyRaw
                val timeTaken = System.currentTimeMillis() - startTime
                error("[" + timeTaken + "] [q" + relaxLevel + "] [na/na] [" + clip.toString + "]->[" + httpReq.uri + " -d "+reqBody+ "]->[]", e)
                context.parent ! ErrorResponse(e.getMessage, e)
              }
            })
          }
        } catch {
          case e: Throwable =>
            val reqBodyRaw = httpReq.entity.data.asString
            val temp = parseOpt(reqBodyRaw).getOrElse(JNothing)
            val reqBody = if(temp!=JNothing)compact(temp) else reqBodyRaw
            val timeTaken = System.currentTimeMillis() - startTime
            error("[" + timeTaken + "] [q" + relaxLevel + "] [na/na] [" + clip.toString + "]->[" + httpReq.uri + " -d "+reqBody+ "]->[]", e)
            context.parent ! ErrorResponse(e.getMessage, e)
        }

      case response: WrappedResponse =>
        import response.searchParams.limits._
        import response.searchParams.req._
        import response.searchParams.startTime
        import response.searchParams.view._
        import response.{relaxLevel, result}
        import response._
        try {
          val parsedResult = parse(result.toString)

          val reqBodyRaw = httpReq.entity.data.asString
          val temp = parseOpt(reqBodyRaw).getOrElse(JNothing)
          val reqBody = if(temp!=JNothing)compact(temp) else reqBodyRaw
          val timeTaken = System.currentTimeMillis - startTime

          info("[" + result.getTookInMillis + "/" + timeTaken + (if (result.isTimedOut) " timeout" else "") + "] [q" + relaxLevel + "] [" + result.getHits.hits.length + "/" + result.getHits.getTotalHits + (if (result.isTerminatedEarly) " termearly (" + Math.min(maxdocspershard, int("max-docs-per-shard")) + ")" else "") + "] [" + clip.toString + "]->[" + httpReq.uri + " -d "+reqBody+"] ["+w.mkString(" ")+"]")
          context.parent ! SearchResult(result.getHits.hits.length, timeTaken, relaxLevel, w.mkString(" "), if(explain) parse(query.toString) else JObject(), parsedResult)
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

