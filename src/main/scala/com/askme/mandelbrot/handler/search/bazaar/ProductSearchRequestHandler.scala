package com.askme.mandelbrot.handler.search.bazaar

import java.util

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.message.ErrorResponse
import com.askme.mandelbrot.handler.search.bazaar.message.ProductSearchParams
import com.askme.mandelbrot.handler.search.bazaar.message.SearchResult
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
import org.elasticsearch.script.Script
import org.elasticsearch.script.ScriptService.ScriptType
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesMethod
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
object ProductSearchRequestHandler extends Logging {

  val pat = """(?U)[^\p{alnum}]+"""

  private val randomParams = new util.HashMap[String, AnyRef]
  randomParams.put("buckets", int2Integer(5))

  private def getSort(sort: String, mpdm_store_front_id: Int, cities: Array[String], w: Array[String],
                      subscriptionFilter: QueryBuilder, suggest: Boolean, category_id: Int = 0, brands: String=""): List[SortBuilder] = {

    val sorters =
    if(sort=="price.asc") {
      List(fieldSort("min_price").order(SortOrder.ASC))
    }
    else if(sort=="price.desc") {
      List(fieldSort("min_price").order(SortOrder.DESC))
    }
    else {
      (
        (if(cities.nonEmpty && !suggest)
          Some(scriptSort(new Script("docscoreexponent", ScriptType.INLINE, "native", null), "number").order(SortOrder.DESC))
        else
          Some(scoreSort().order(SortOrder.DESC)))::
          (if (mpdm_store_front_id > 0)
            Some(fieldSort("subscriptions.store_fronts.boost").setNestedPath("subscriptions.store_fronts").order(SortOrder.DESC)
              .setNestedFilter(
                boolQuery()
                  .must(termQuery("subscriptions.store_fronts.mpdm_id", mpdm_store_front_id))
                  .must(termQuery("subscriptions.store_fronts.mapping_status", 1))
                  //.must(termQuery("subscriptions.store_fronts.status", 1))
              )
            ) else None) ::
          (if(category_id>0)
            Some(fieldSort("categories_nested.boost").setNestedPath("categories_nested").order(SortOrder.DESC)
              .setNestedFilter(termQuery("categories_nested.category_id", category_id))
              .sortMode("max").missing(0)
            )
          else None)::
          (if(brands.trim.nonEmpty)
            Some(fieldSort("brand_boost").order(SortOrder.DESC).sortMode("max").missing(0))
          else None)::
          Some(fieldSort("subscriptions.boost").setNestedPath("subscriptions").order(SortOrder.DESC)
            .setNestedFilter(subscriptionFilter)
            .sortMode("max").missing(0))::
          (if (cities.nonEmpty)
            Some(fieldSort("subscriptions.is_ndd").setNestedPath("subscriptions").order(SortOrder.DESC)
              .setNestedFilter(
                boolQuery()
                  .must(subscriptionFilter)
                  .must(termsQuery("subscriptions.ndd_city.exact",cities:_*))
              ).sortMode("max").missing(0))
          else
            Some(fieldSort("subscriptions.is_ndd").setNestedPath("subscriptions").order(SortOrder.DESC)
              .setNestedFilter(subscriptionFilter)
              .sortMode("max").missing(0)
            )
            ) ::
          Some(scoreSort().order(SortOrder.DESC))::
          Some(fieldSort("order_count").order(SortOrder.DESC).sortMode("max").missing(0))::
          Some(fieldSort("order_gsv").order(SortOrder.DESC).sortMode("max").missing(0))::
          Some(fieldSort("product_id").order(SortOrder.DESC))::
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
          fuzzyQuery(field, x).prefixLength(fuzzyprefix).fuzziness(if(x.length > 8) Fuzziness.TWO else Fuzziness.ONE))
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
  val forceSpan = Map("name.exact"->"name", "description.exact"->"description",
    "categories.description.exact"->"categories.description", "categories.name.exact"->"category.name")

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
          .fuzziness(if(word.length > 8) Fuzziness.TWO else Fuzziness.ONE)
          .boost(if(word.length > 8) exactBoost/3f else exactBoost/2f)
      else
        termQuery(field, word).boost(exactBoost)

  }

  private def analyze(esClient: Client, index: String, field: String, text: String): Array[String] =
    new AnalyzeRequestBuilder(esClient.admin.indices, AnalyzeAction.INSTANCE, index, text).setField(field).get().getTokens.map(_.getTerm).toArray


  private val searchFields2 = Map(
    "name" -> 1e8f,
    "categories.name" -> 1e5f,
    "seller_name" -> 1e4f,
    "attributes_value" -> 1e7f)

  private val fullFields2 = Map(
    "name.exact"->1e10f,
    "categories.name.exact"->1e6f,
    "seller_name.exact" -> 1e5f,
    "attributes_value.exact" -> 1e9f)


  private val emptyStringArray = new Array[String](0)

  private def superBoost(len: Int) = math.pow(10, math.min(10,len+1)).toFloat

  private case class WrappedResponse(searchParams: ProductSearchParams, result: SearchResponse, query: SearchRequestBuilder, relaxLevel: Int)
  private case class ReSearch(searchParams: ProductSearchParams, filter: BoolQueryBuilder, search: SearchRequestBuilder, query: QueryBuilder, relaxLevel: Int, response: SearchResponse)

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



class ProductSearchRequestHandler(val config: Config, serverContext: SearchContext) extends Actor with Configurable with Logging {
  import ProductSearchRequestHandler._

  private val esClient: Client = serverContext.esClient
  private var w = emptyStringArray
  private var subscriptionFilter: BoolQueryBuilder = null
  private val matchedSubscriptionFilter =
    boolQuery()
      .must(termQuery("subscriptions.status", 1))
      .must(rangeQuery("subscriptions.quantity").gt(0))

  private def buildFilter(searchParams: ProductSearchParams, externalFilter: JValue): BoolQueryBuilder = {
    import searchParams.filters._
    import searchParams.idx._
    import searchParams.view._
    import searchParams.page._
    implicit val formats = org.json4s.DefaultFormats

    // filters
    val finalFilter = boolQuery()
    if(externalFilter!=JNothing)
      finalFilter.must(QueryBuilders.wrapperQuery(compact(externalFilter)))

    finalFilter.must(termQuery("status", 1))
    if (product_id != 0) {
      finalFilter.must(termQuery("product_id", product_id))
    }
    if (base_id != 0) {
      finalFilter.must(termQuery("base_product_id", base_id))
    }

    if(price_min>0f)
      if(price_max>0f)
        finalFilter.must(rangeQuery("min_price").from(price_min).to(price_max))
      else
        finalFilter.must(rangeQuery("min_price").from(price_min))
    else if(price_max>0f)
      finalFilter.must(rangeQuery("min_price").to(price_max))

    val subscriptionFilter =
      boolQuery()
        .must(rangeQuery("subscriptions.subscribed_product_id").gt(0))

    this.subscriptionFilter =  boolQuery()
      .must(rangeQuery("subscriptions.subscribed_product_id").gt(0))

    if(grouped_id != 0) {
      subscriptionFilter.must(
        termQuery("subscriptions.product_id", grouped_id)
      )
      this.subscriptionFilter.must(
        termQuery("subscriptions.product_id", grouped_id)
      )
    }

    if(subscribed_id.nonEmpty ) {
      val q = boolQuery().shouldAll(subscribed_id.map(termQuery("subscriptions.subscribed_product_id", _)))
      subscriptionFilter.must(q)
      this.subscriptionFilter.must(q)
    } else {
      subscriptionFilter
        .must(termQuery("subscriptions.status", 1))
        .must(rangeQuery("subscriptions.quantity").gt(0))
      this.subscriptionFilter
        .must(termQuery("subscriptions.status", 1))
        .must(rangeQuery("subscriptions.quantity").gt(0))
    }


    if(crm_seller_id !=0) {
      subscriptionFilter.must(termQuery("subscriptions.crm_seller_id", crm_seller_id))
      this.subscriptionFilter.must(termQuery("subscriptions.crm_seller_id", crm_seller_id))
    }

    if (national_only) {
      subscriptionFilter.must(termQuery("subscriptions.is_ndd", 0))
      this.subscriptionFilter.must(termQuery("subscriptions.is_ndd", 0))
    } else if (ndd_only) {
      subscriptionFilter.must(termQuery("subscriptions.is_ndd", 1))
      this.subscriptionFilter.must(termQuery("subscriptions.is_ndd", 1))
    }

    if (city != "") {
      val cityFilter = boolQuery
      city.split( """,""").map(analyze(esClient, index, "subscriptions.ndd_city.exact", _).mkString(" ")).filter(!_.isEmpty).foreach { c =>
        cityFilter.should(termQuery("subscriptions.ndd_city.exact", c))
      }

      if(cityFilter.hasClauses) {
        subscriptionFilter.must(boolQuery().should(cityFilter).should(termQuery("subscriptions.is_ndd", 0)))
        this.subscriptionFilter.must(boolQuery().should(cityFilter).should(termQuery("subscriptions.is_ndd", 0)))
        matchedSubscriptionFilter.must(boolQuery().should(cityFilter).should(termQuery("subscriptions.is_ndd", 0)))
      }
    }


    if(store!="") {
      val storeNames = store.toLowerCase.trim.split(",")
      finalFilter.must(termsQuery("stores.name", storeNames:_*))
      if(storeNames.contains("flash")) {
        subscriptionFilter
          .must(rangeQuery("subscriptions.flash_sale_start_date").lte("now"))
          .must(rangeQuery("subscriptions.flash_sale_end_date").gte("now"))
        this.subscriptionFilter
          .must(rangeQuery("subscriptions.flash_sale_start_date").lte("now"))
          .must(rangeQuery("subscriptions.flash_sale_end_date").gte("now"))
      }
    }

    if(store_front_id > 0) {
      subscriptionFilter.must(
        nestedQuery("subscriptions.store_fronts", boolQuery()
          .must(termQuery("subscriptions.store_fronts.id", store_front_id))
          .must(termQuery("subscriptions.store_fronts.mapping_status", 1))
          //.must(termQuery("subscriptions.store_fronts.status", 1))
          )
      )
    }

    if(mpdm_store_front_id != 0) {
      if(mpdm_store_front_id>0) {
        val f = boolQuery()
          .must(termQuery("subscriptions.store_fronts.mpdm_id", mpdm_store_front_id))
          .must(termQuery("subscriptions.store_fronts.mapping_status", 1))
          //.must(termQuery("subscriptions.store_fronts.status", 1))
        subscriptionFilter.must(
          nestedQuery("subscriptions.store_fronts", f)
        )
      }
      else {
        val f = boolQuery()
          .must(termQuery("subscriptions.store_fronts.mapping_status", 1))
          //.must(termQuery("subscriptions.store_fronts.status", 1))
        subscriptionFilter.must(
          nestedQuery("subscriptions.store_fronts", f)
        )
      }

    }


    if(optionFilters.nonEmpty) {
      optionFilters.foreach { filter =>
        val b = boolQuery().must(termQuery("subscriptions.options.name.agg", filter._1))
        val sub = boolQuery
        filter._2.split("""#""").map(analyze(esClient, index, "subscriptions.options.values.exact", _).mkString(" ")).filter(!_.isEmpty).foreach { br =>
          sub.should(termQuery("subscriptions.options.values.exact", br))
        }
        if(sub.hasClauses)
          b.must(sub)
        if(b.hasClauses) {
          subscriptionFilter.must(nestedQuery("subscriptions.options", b))
        }
      }
    }

    if(subscriptionFilter.hasClauses) {
      finalFilter.must(
        nestedQuery("subscriptions", subscriptionFilter)
          .innerHit(
            new QueryInnerHitBuilder().setName("best_subscription")
              .addSort("subscriptions.min_price", SortOrder.ASC)
              .addSort("subscriptions.is_ndd", SortOrder.DESC)
              .addSort("subscriptions.boost", SortOrder.DESC)
              .addSort("subscriptions.order_count", SortOrder.DESC)
              .setFrom(subscriptions_offset).setSize(subscriptions_size)
              .setFetchSource("*", null).setExplain(explain)))
    }


    if (category != "") {
      val b = boolQuery
      category.split("""#""").map(analyze(esClient, index, "categories_nested.name.exact", _).mkString(" ")).filter(!_.isEmpty).foreach { cat =>
        b.should(termQuery("categories_nested.name.exact", cat))
      }
      if(b.hasClauses)
        finalFilter.must(nestedQuery("categories_nested", b))
    }

    if (category_id > 0) {
      finalFilter.must(nestedQuery("categories_nested", termQuery("categories_nested.category_id", category_id)))
    }

    if (brand != "") {
      val b = boolQuery.must(termQuery("attributes.name.agg", "Filter_Brand"))
      val sub = boolQuery
      brand.split("""#""").map(analyze(esClient, index, "attributes.name.exact", _).mkString(" ")).filter(!_.isEmpty).foreach { br =>
        sub.should(termQuery("attributes.value.exact", br))
      }
      b.must(sub)
      if(b.hasClauses)
        finalFilter.must(nestedQuery("attributes", b))
    }

    if(filters.nonEmpty) {
      filters.foreach { filter =>
        val b = boolQuery().must(termQuery("attributes.name.agg", filter._1))
        val sub = boolQuery
        filter._2.split("""#""").map(analyze(esClient, index, "attributes.value.exact", _).mkString(" ")).filter(!_.isEmpty).foreach { br =>
          sub.should(termQuery("attributes.value.exact", br))
        }
        if(sub.hasClauses)
          b.must(sub)
        if(b.hasClauses)
          finalFilter.must(nestedQuery("attributes", b))
      }
    }

    finalFilter
  }

  private def buildSearch(searchParams: ProductSearchParams): SearchRequestBuilder = {
    import searchParams.idx._
    import searchParams.limits._
    import searchParams.page._
    import searchParams.view._
    import searchParams.filters._
    import searchParams.text._

    val sorters = getSort(
      sort, mpdm_store_front_id,
      city.split( """,""").map(analyze(esClient, index, "subscriptions.ndd_city.exact", _).mkString(" ")).filter(!_.isEmpty),
      w, subscriptionFilter, suggest, category_id, if(brand.nonEmpty)brand else filters.getOrDefault("Filter_Brand", ""))

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

    if(subscribed_id.nonEmpty) {

      search.addInnerHit("matched_subscriptions",
        new InnerHitsBuilder.InnerHit()
          .setQuery(matchedSubscriptionFilter)
          .setPath("subscriptions")
          .setFetchSource("*", null)
          .setFrom(subscriptions_offset).setSize(subscriptions_size)
      )
    }

    if (agg) {
      val scoreSorter = max("score").script(new Script("docscore", ScriptType.INLINE, "native", new util.HashMap[String, AnyRef]))
      val orderSorter = sum("order").field("order_count")


      search.addAggregation(
        terms("categories").field("categories.name.agg").size(if(suggest) 2 else aggbuckets).order(
          Terms.Order.compound(
            Terms.Order.aggregation("score", false),
            Terms.Order.aggregation("order", false),
            Terms.Order.count(false)
          )
        )
          .subAggregation(scoreSorter)
          .subAggregation(orderSorter)
      )

      search.addAggregation(
        nested("categories_l2").path("categories_l5").subAggregation(
          filter("categories_l2").filter(boolQuery().mustNot(termQuery("categories_l5.name.exact", "root catalog"))).subAggregation(
          terms("categories").field("categories_l5.name.agg").size(if(suggest) 3 else aggbuckets).order(
            Terms.Order.compound(
              Terms.Order.aggregation("score", false),
              Terms.Order.aggregation("rev>order", false),
              Terms.Order.count(false)
            )
          )
            .subAggregation(scoreSorter)
            .subAggregation(reverseNested("rev").subAggregation(orderSorter))
        )
        )
      )

      search.addAggregation(
        nested("l3").path("categories_tree.categories_tree.categories_tree.categories_tree").subAggregation(
          terms("id").field("categories_tree.categories_tree.categories_tree.categories_tree.category_id").size(if(suggest) 2 else aggbuckets).subAggregation(
            terms("name").field("categories_tree.categories_tree.categories_tree.categories_tree.name.agg").size(1)
          ).subAggregation(
            nested("l4").path("categories_tree.categories_tree.categories_tree.categories_tree.categories_tree").subAggregation(
              terms("id").field("categories_tree.categories_tree.categories_tree.categories_tree.categories_tree.category_id").size(if(suggest) 2 else aggbuckets).subAggregation(
                terms("name").field("categories_tree.categories_tree.categories_tree.categories_tree.categories_tree.name.agg").size(1)
              ).subAggregation(
                nested("l5").path("categories_tree.categories_tree.categories_tree.categories_tree.categories_tree.categories_tree").subAggregation(
                  terms("id").field("categories_tree.categories_tree.categories_tree.categories_tree.categories_tree.categories_tree.category_id").size(if(suggest) 2 else aggbuckets).subAggregation(
                    terms("name").field("categories_tree.categories_tree.categories_tree.categories_tree.categories_tree.categories_tree.name.agg").size(1)
                  )
                )
              )
            )
          )
        )
      )


      search.addAggregation(
        nested("subscriptions").path("subscriptions")
          .subAggregation(
            filter("matched").filter(subscriptionFilter)
              .subAggregation(
                nested("options").path("subscriptions.options").subAggregation(
                  terms("name").field("subscriptions.options.name.agg").size(aggbuckets).subAggregation(
                    terms("values").field("subscriptions.options.values.agg").size(aggbuckets).order(Terms.Order.term(true))
                  )
                )
              )
              .subAggregation(filter("ndd").filter(termQuery("subscriptions.is_ndd", 1)))
              .subAggregation(
                nested("store_fronts").path("subscriptions.store_fronts").subAggregation(
                  filter("store_fronts").filter(boolQuery()
                    .must(termQuery("subscriptions.store_fronts.mapping_status", 1))
                    //.must(termQuery("subscriptions.store_fronts.status", 1))
                    )
                    .subAggregation(
                      terms("mpdm_id").field("subscriptions.store_fronts.mpdm_id").size(if(mpdm_store_front_id<0) 100 else 10).order(
                        Terms.Order.aggregation("rev>filter>order", false)
                      )
                        .subAggregation(terms("name").field("subscriptions.store_fronts.title.agg").size(1).order(Terms.Order.count(false)))
                        .subAggregation(
                          reverseNested("rev").subAggregation(
                            filter("filter").filter(rangeQuery("order_last_dt").gte("now-2w")).subAggregation(orderSorter)
                          )
                        )
                    )
                )
              )
          )
      )

      search.addAggregation(
        nested("attributes").path("attributes")
          .subAggregation(
            filter("filters").filter(prefixQuery("attributes.name.agg", "Filter")).subAggregation(
              terms("attributes").field("attributes.name.agg").size(aggbuckets)
                .subAggregation(terms("vals").field("attributes.value.agg").size(aggbuckets)))))

      search.addAggregation(stats("price_stats").field("min_price"))
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
                "name" -> 1e12f,
                "categories.name" -> 1e11f,
                "attributes_value" -> 1e11f),
              w, w.length, 1, fuzzy = false, sloppy = false, tokenRelax = 0
            )
          )/*.add(
            shinglePartitionSuggest(
              Map("name" -> 1e6f,
                "categories.name" -> 1e5f,
                "attributes_value" -> 1e5f),
              w, w.length, 1, fuzzy = true, sloppy = true, tokenRelax = 0
            )
          )*/.add(
            shinglePartitionSuggest(
              Map("name.token_edge_ngram" -> 1e2f,
                "categories.name.token_edge_ngram" -> 1e1f,
                "attributes_value.token_edge_ngram" -> 1e2f),
              w, w.length, 1, fuzzy = false, sloppy = true, tokenRelax = 0
            )
          ).add(
            shinglePartitionSuggest(
              Map("name.shingle_nospace_edge_ngram" -> 1e2f,
                "categories.name.shingle_nospace_edge_ngram" -> 1e1f,
                "attributes_value.shingle_nospace_edge_ngram" -> 1e2f),
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
              Map("name" -> 1e12f,
                "categories.name" -> 1e11f,
                "attributes_value" -> 1e11f),
              w, w.length, 1, fuzzy = false, sloppy = false, tokenRelax = 0
            )
          )/*.add(
            shinglePartitionSuggest(
              Map("name" -> 1e6f,
                "categories.name" -> 1e5f,
                "attributes_value" -> 1e5f),
              w, w.length, 1, fuzzy = true, sloppy = true, tokenRelax = 0
            )
          )*/.add(
            shinglePartitionSuggest(
              Map("name.token_edge_ngram" -> 1e2f,
                "categories.name.token_edge_ngram" -> 1e1f,
                "attributes_value.token_edge_ngram" -> 1e2f),
              w, w.length, 1, fuzzy = false, sloppy = true, tokenRelax = 0
            )
          ).add(
            shinglePartitionSuggest(
              Map("name.shingle_nospace_edge_ngram" -> 1e2f,
                "categories.name.shingle_nospace_edge_ngram" -> 1e1f,
                "attributes_value.shingle_nospace_edge_ngram" -> 1e2f),
              w, w.length, 1, fuzzy = false, sloppy = true, tokenRelax = 0
            )
          )

          val q2 = boolQuery
          if (front.nonEmpty) {
            q2.must(
              disMaxQuery.add(
                shinglePartitionSuggest(
                  Map("name" -> 1e12f,
                    "categories.name" -> 1e11f,
                    "attributes_value" -> 1e11f),
                  front, front.length, 1, fuzzy = false, sloppy = false, tokenRelax = 0
                )
              )/*.add(
                shinglePartitionSuggest(
                  Map("name" -> 1e6f,
                    "categories.name" -> 1e5f,
                    "attributes_value" -> 1e5f),
                  front, front.length, 1, fuzzy = true, sloppy = true, tokenRelax = 0
                )
              )*/.add(
                shinglePartitionSuggest(
                  Map("name.token_edge_ngram" -> 1e2f,
                    "categories.name.token_edge_ngram" -> 1e1f,
                    "attributes_value.token_edge_ngram" -> 1e2f),
                  front, front.length, 1, fuzzy = false, sloppy = true, tokenRelax = 0
                )
              ).add(
                shinglePartitionSuggest(
                  Map("name.shingle_nospace_edge_ngram" -> 1e2f,
                    "categories.name.shingle_nospace_edge_ngram" -> 1e1f,
                    "attributes_value.shingle_nospace_edge_ngram" -> 1e2f),
                  front, front.length, 1, fuzzy = false, sloppy = true, tokenRelax = 0
                )
              )
            )
          }

          val q3 = disMaxQuery
            .add(fuzzyOrTermQuery("name.token_edge_ngram", last_raw, 1e19f, 1, fuzzy = true))
            //.add(fuzzyOrTermQuery("name.shingle_nospace_edge_ngram", last_raw, 1e17f, 1, fuzzy = true))

          q3.add(fuzzyOrTermQuery("categories.name.token_edge_ngram", last_raw, 1e17f, 1, fuzzy = false))
            //.add(fuzzyOrTermQuery("categories.name.shingle_nospace_edge_ngram", last_raw, 1e15f, 1, fuzzy = false))

          q3.add(fuzzyOrTermQuery("attributes_value.token_edge_ngram", last_raw, 1e17f, 1, fuzzy = false))
            //.add(fuzzyOrTermQuery("attributes_value.shingle_nospace_edge_ngram", last_raw, 1e15f, 1, fuzzy = false))


          q.add(if (q2.hasClauses) q2.must(q3) else q3)
        } else
          matchAllQuery
      }
    query
  }

  override def receive = {
      case searchParams: ProductSearchParams =>
        import searchParams.filters._
        import searchParams.idx._
        import searchParams.req._
        import searchParams.startTime
        import searchParams.text._

        try {
          val externalString = httpReq.entity.data.asString
          val externals = parseOpt(externalString).getOrElse(JNothing)

          w = analyze(esClient, index, "name", kw)
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
                me ! WrappedResponse(searchParams, response, search, 0)
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
            context.self ! WrappedResponse(searchParams, response, search, relaxLevel - 1)
          else {
            val query = qDefs(relaxLevel)._1(w, w.length)
            val leastCount = qDefs(relaxLevel)._2
            val me = context.self

            val qfinal = boolQuery.must(query).filter(filter)
            search.setQuery(qfinal)
            search.execute(new ActionListener[SearchResponse] {
              override def onResponse(response: SearchResponse): Unit = {
                if (response.getHits.totalHits() >= leastCount || relaxLevel >= qDefs.length - 1)
                  me ! WrappedResponse(searchParams, response, search, relaxLevel)
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

