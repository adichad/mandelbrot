package com.askme.mandelbrot.handler.search

import java.net.URLEncoder

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.EmptyResponse
import com.askme.mandelbrot.handler.search.message.{SearchParams, SearchResult}
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse, SearchType}
import org.elasticsearch.action.suggest.SuggestRequest
import org.elasticsearch.client.Client
import org.elasticsearch.common.geo.GeoDistance
import org.elasticsearch.common.unit.{Fuzziness, TimeValue}
import org.elasticsearch.index.query.FilterBuilders._
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.index.query._
import org.elasticsearch.search.aggregations.{AggregationBuilder, AbstractAggregationBuilder}
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.aggregations.bucket.nested.Nested
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder
import org.elasticsearch.search.sort._
import org.elasticsearch.search.suggest.SuggestBuilders
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConversions._
import scala.util.control.TailCalls.TailRec


/**
 * Created by adichad on 08/01/15.
 */


object PlaceSearchRequestHandler extends Logging {

  val pat = """(?U)[^\p{alnum}]+"""
  val idregex = """[uU]\d+[lL]\d+""".r

  private def getSort(sort: String, lat: Double = 0d, lon: Double = 0d, areaSlugs: String = "") = {
    val parts = for (x <- sort.split(",")) yield x.trim
    parts.map(
      _ match {
        case "_score" => new ScoreSortBuilder().order(SortOrder.DESC)
        case "_distance" => SortBuilders.scriptSort("geobucket", "number").lang("native")
          .param("lat", lat).param("lon", lon).param("areaSlugs", areaSlugs).order(SortOrder.ASC)
        case "_ct" => SortBuilders.scriptSort("customertype", "number").lang("native").order(SortOrder.ASC)
        case "_mc" => SortBuilders.scriptSort("mediacountsort", "number").lang("native").order(SortOrder.DESC)
        case x =>
          val pair = x.split( """\.""", 2)
          if (pair.size == 2)
            new FieldSortBuilder(pair(0)).order(SortOrder.valueOf(pair(1)))
          else
            new FieldSortBuilder(pair(0)).order(SortOrder.DESC)
      }
    )
  }


  private[PlaceSearchRequestHandler] def nestIfNeeded(fieldName: String, q: BaseQueryBuilder): BaseQueryBuilder = {
    /*val parts = fieldName.split("""\.""")
    if (parts.length > 1)
      nestedQuery(parts(0), q).scoreMode("max")
    else
    */
    q
  }



  private implicit class DisMaxQueryPimp(val q: DisMaxQueryBuilder) {
    def addAll(queries: Iterable[QueryBuilder]) = {
      queries.foreach(q.add)
      q
    }
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

  private implicit class SearchPimp(val search: SearchRequestBuilder) {
    def addSorts(sorts: Iterable[SortBuilder]) = {
      sorts.foreach(search.addSort)
      search
    }

    def addAggregations(aggregations: Iterable[AbstractAggregationBuilder]) = {
      aggregations.foreach(search.addAggregation)
      search
    }
  }

  private implicit class AggregationPimp(val agg: TopHitsBuilder) {
    def addSorts(sorts: Iterable[SortBuilder]) = {
      sorts.foreach(agg.addSort)
      agg
    }
  }

  private[PlaceSearchRequestHandler] def shingleSpan(field: String, boost: Float, w: Array[String], fuzzyprefix: Int, maxShingle: Int, minShingle: Int = 1, sloppy: Boolean = true, fuzzy: Boolean = true) = {
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
      disMaxQuery.addAll(tokenFields.map(field => shingleSpan(field._1, field._2, w, 1, w.length, w.length, sloppy, fuzzy)))
    else
      disMaxQuery.addAll(recomFields.map(field => shingleFull(field._1, field._2, w, 1, w.length, math.max(w.length-tokenRelax, 1), fuzzy)))
  }

  private def shinglePartition(tokenFields: Map[String, Float], recomFields: Map[String, Float], w: Array[String],
                               maxShingle: Int, minShingle: Int = 1, fuzzy: Boolean = false, sloppy: Boolean = false,
                               span: Boolean = false, tokenRelax: Int = 0): BoolQueryBuilder = {

    if(w.length>0)
      boolQuery.minimumNumberShouldMatch(1).shouldAll(
        (math.min(minShingle, w.length) to math.min(maxShingle, w.length)).map(len=>(w.slice(0, len), w.slice(len, w.length))).map(x =>
          if(x._2.length>0)
            shinglePartition(tokenFields, recomFields, x._2, maxShingle, minShingle, fuzzy, sloppy, span, tokenRelax)
              .must(currQuery(tokenFields, recomFields, x._1, fuzzy, sloppy, span, tokenRelax))
          else
            currQuery(tokenFields, recomFields, x._1, fuzzy, sloppy, span, tokenRelax)
        )
      )
    else
      boolQuery
  }

  private def strongMatch(fields: Map[String, Float],
                          condFields: Map[String, Map[String, Map[String, Float]]],
                          w: Array[String], kw: String, fuzzyprefix: Int, fuzzysim: Float, esClient: Client, index: String) = {

    val allQuery = boolQuery.minimumNumberShouldMatch(math.ceil(w.length.toFloat * 4f / 5f).toInt)
    //var i = 1000000
    w.foreach {
      word => {
        val posBoost = 1//math.max(1, i)
        val wordQuery = boolQuery
        fields.foreach {
          field =>
            //wordQuery.should(nestIfNeeded(field._1, fuzzyQuery(field._1, word).prefixLength(fuzzyprefix).fuzziness(Fuzziness.ONE)))
            wordQuery.should(nestIfNeeded(field._1, fuzzyOrTermQuery(field._1, word, field._2 * posBoost, fuzzyprefix)))
        }
        condFields.foreach {
          cond: (String, Map[String, Map[String, Float]]) => {
            cond._2.foreach {
              valField: (String, Map[String, Float]) => {
                val perQuestionQuery = boolQuery
                perQuestionQuery.must(termQuery(cond._1, valField._1))
                val answerQuery = boolQuery
                valField._2.foreach {
                  subField: (String, Float) =>
                    //answerQuery.should(fuzzyQuery(subField._1, word).prefixLength(fuzzyprefix).fuzziness(Fuzziness.ONE))
                    answerQuery.should(fuzzyOrTermQuery(subField._1, word, subField._2 * posBoost, fuzzyprefix))
                }
                perQuestionQuery.must(answerQuery)
                wordQuery.should(nestIfNeeded(cond._1, perQuestionQuery))
              }
            }
          }
        }
        allQuery.should(wordQuery)
        //i /= 10
      }
    }
    allQuery
  }

  private def fuzzyOrTermQuery(field: String, word: String, exactBoost: Float, fuzzyPrefix: Int, fuzzy: Boolean = true) = {
      if(word.length > 8 && fuzzy)
        fuzzyQuery(field, word).prefixLength(fuzzyPrefix)
          .fuzziness(if(word.length > 12) Fuzziness.TWO else Fuzziness.ONE)
          .boost(if(word.length > 12) exactBoost/3f else exactBoost/2f)
      else
        termQuery(field, word).boost(exactBoost)

  }

  private def matchAnalyzed(esClient: Client, index: String, field: String, text: String, keywords: Array[String]): Boolean = {
    if(keywords.isEmpty) false else analyze(esClient, index, field, text).deep == keywords.deep
  }

  private def weakMatchAnalyzed(esClient: Client, index: String, field: String, text: String, keywords: Array[String]): Boolean = {
    val textWords = analyze(esClient, index, field, text)
    if(keywords.length > textWords.length) false else textWords.zip(keywords).forall(x=>x._1==x._2)
  }

  private val catFilterFields = Set("Product.l3categoryexact" -> 1024f, "Product.categorykeywordsexact" -> 1024f, "Product.l2categoryexact" -> 128f, "Product.l1categoryexact" -> 64f)

  private val catFilterFieldsShingle = Set("Product.l3categoryshingle" -> 64f, "Product.categorykeywordsshingle" -> 64f, "Product.l2categoryshingle" -> 8f, "Product.l1categoryshingle" -> 4f)


  private case class CategoryFilter(filter: FilterBuilder, cats: Seq[String])

  private def categoryFilter(mw: Array[String], cityFilter: BoolFilterBuilder, aggBuckets: Int, esClient: Client, index: String, esType: String, maxdocspershard: Int): CategoryFilter = {
    val cquery = disMaxQuery
    var hasClauses = false

    catFilterFields.foreach {
      field: (String, Float) => {
        (1 to mw.length).foreach { len =>
          mw.sliding(len).foreach { shingle =>
            val ck = shingle.mkString(" ")
            if(ck.trim != "") {
              cquery.add(nestIfNeeded(field._1, termQuery(field._1, ck).boost(field._2 * len)))
              hasClauses = true
            }
          }
        }
      }
    }

    catFilterFieldsShingle.foreach {
      field: (String, Float) => {
        (math.min(2, math.max(1, mw.length)) to math.min(3, math.max(1, mw.length))).foreach { len =>
          mw.sliding(len).foreach { shingle =>
            val ck = shingle.mkString(" ")
            if(ck.trim != "") {
              cquery.add(nestIfNeeded(field._1, termQuery(field._1, ck).boost(field._2 * len)))
              hasClauses = true
            }
          }
        }
      }
    }

    if(hasClauses) {

      val catFilter = boolFilter.cache(false)
      val cats = esClient.prepareSearch(index.split(","): _*).setQueryCache(true)
        .setTypes(esType.split(","): _*)
        .setSearchType(SearchType.QUERY_THEN_FETCH)
        .setQuery(filteredQuery(if (cityFilter.hasClauses) filteredQuery(cquery, cityFilter) else cquery, boolFilter.mustNot(termFilter("DeleteFlag", 1l))))
        .setTerminateAfter(5000)
        .setFrom(0).setSize(0)
        .setTimeout(TimeValue.timeValueMillis(500))
        .addAggregation(terms("categories").field("Product.l3categoryaggr").size(2).order(Terms.Order.aggregation("max_score", false))
        .subAggregation(max("max_score").script("docscore").lang("native")))
        .execute().get()
        .getAggregations.get("categories").asInstanceOf[Terms]
        .getBuckets.map(_.getKey)

      cats.map(termFilter("Product.l3categoryaggr", _).cache(false)).foreach(catFilter.should(_))

      //debug(catFilter.toString)
      if (catFilter.hasClauses) {
        catFilter.should(queryFilter(shingleSpan("LocationName", 1f, mw, 1, mw.length, mw.length)).cache(false))
        catFilter.should(queryFilter(shingleSpan("CompanyAliases", 1f, mw, 1, mw.length, mw.length)).cache(false))
        //catFilter.should(queryFilter(shingleSpan("Product.stringattribute.answer", 1f, mw, 1, mw.length, mw.length)).cache(false))
        catFilter.should(queryFilter(nestIfNeeded("Product.l3categoryexact", termQuery("Product.l3categoryexact", mw.mkString(" ")))).cache(true))
        catFilter.should(queryFilter(nestIfNeeded("Product.categorykeywordsexact", termQuery("Product.categorykeywordsexact", mw.mkString(" ")))).cache(false))

        Seq("LocationNameExact", "CompanyAliasesExact", "Product.stringattribute.answerexact").foreach {
          field: (String) => {
            (math.max(1,mw.length/2) to mw.length).foreach { len =>
              mw.sliding(len).foreach { shingle =>
                val ck = shingle.mkString(" ")
                if(ck.trim != "") {
                  cquery.add(nestIfNeeded(field, termQuery(field, ck)))
                  hasClauses = true
                }
              }
            }
          }
        }

        catFilter.should(queryFilter(cquery).cache(false))
        CategoryFilter(catFilter, cats)
      }
      else
        CategoryFilter(null, Seq[String]())
    }
    else CategoryFilter(null, Seq[String]())
  }

  private def analyze(esClient: Client, index: String, field: String, text: String): Array[String] =
    new AnalyzeRequestBuilder(esClient.admin.indices, index, text).setField(field).get().getTokens.map(_.getTerm).toArray


  private val searchFields = Map(
    "LocationName" -> 81920f, "CompanyAliases" -> 81920f,
    "Product.l3category" -> 40960f,
    "Product.l2category" -> 1024f,
    "Product.l1category" -> 512f,
    "LocationType"->1024f,
    "BusinessType"->1024f,
    "Product.name" -> 1024f,
    "Product.brand" -> 2048f,
    "Product.categorykeywords" -> 40960f,
    "Product.stringattribute.answer" -> 512f,
    "Area"->8f, "AreaSynonyms"->8f,
    "City"->1f, "CitySynonyms"->1f)

  private val condFields = Map(
    "Product.stringattribute.question" -> Map(
      "brand" -> Map("Product.stringattribute.answer" -> 2048f),
      "menu" -> Map("Product.stringattribute.answer" -> 2048f),
      "destination" -> Map("Product.stringattribute.answer" -> 2048f),
      "product" -> Map("Product.stringattribute.answer" -> 2048f)
    )
  )

  private val exactFields = Map(
    "CompanyAliases" -> 209715200f,
    "Product.categorykeywords" -> 104857600f)

  private val exactFirstFields = Map(
    "LocationName" -> 209715200f,
    "DetailSlug" -> 209715200f,
    "Product.l3category" -> 10240f)

  private val fullExactFields = Map(
    "LocationNameExact"->409715200000f, "CompanyAliasesExact"->409715200000f,
    "Product.l3categoryexact"->104857600000f,
    "Product.l2categoryexact"->10485760000f,
    "Product.l1categoryexact"->1048576000f,
    "Product.categorykeywordsexact"->104857600000f,
    "Product.stringattribute.answerexact"->1f)

  private val fullFields = Map(
    "LocationNameExact"->20971520f, "CompanyAliasesExact"->20971520f,
    "Product.l3categoryexact"->2097152f,
    "Product.l2categoryexact"->1048576f,
    "Product.l1categoryexact"->1048576f,
    "Product.categorykeywordsexact"->2097152f,
    "Product.stringattribute.answerexact"->1f)


  private val fullFields2 = Map(
    "LocationNameExact"->100000000f, "CompanyAliasesExact"->100000000f,
    "Product.l3categoryexact"->10000000000f,
    "Product.l2categoryexact"->10000000f,
    "Product.l1categoryexact"->10000000f,
    "Product.categorykeywordsexact"->10000000000f,
    "Product.stringattribute.answerexact"->1000000f)

  private val searchFields2 = Map(
    "LocationName" -> 10000000f, "CompanyAliases" -> 10000000f,
    "Product.l3category" -> 10000000f,
    "Product.l2category" -> 1000f,
    "Product.l1category" -> 100f,
    "LocationType"->1000f,
    "BusinessType"->1000f,
    "Product.name" -> 1000f,
    "Product.brand" -> 10000f,
    "Product.categorykeywords" -> 10000000f,
    "Product.stringattribute.answer" -> 100f,
    "Area"->10f, "AreaSynonyms"->10f,
    "City"->1f, "CitySynonyms"->1f)


  private val emptyStringArray = new Array[String](0)

  private def superBoost(len: Int) = math.pow(10, math.min(10,len-1)).toFloat * (searchFields.size + condFields.values.size + 1)
  private val paidFactor = 1e15f




  private case class WrappedResponse(searchParams: SearchParams, result: SearchResponse, relaxLevel: Int)
  private case class ReSearch(searchParams: SearchParams, filter: FilterBuilder, search: SearchRequestBuilder, relaxLevel: Int, response: SearchResponse)

  private def queryBuilder(fuzzy: Boolean = false, sloppy: Boolean = false, span: Boolean = false, tokenRelax: Int = 0)
                          (tokenFields: Map[String, Float], recomFields: Map[String, Float], w: Array[String], maxShingle: Int, minShingle: Int = 1) = {
    shinglePartition(tokenFields, recomFields, w, maxShingle, minShingle, fuzzy, sloppy, span, tokenRelax)
  }

  private val qDefs: Seq[(Map[String, Float], Map[String, Float], Array[String], Int, Int)=>BaseQueryBuilder] = Seq(
    queryBuilder(false, false, false, 0),
    queryBuilder(false, false, true, 0),
    queryBuilder(false, false, false, 1),
    queryBuilder(true, false, false, 0),
    queryBuilder(false, true, true, 0),
    queryBuilder(true, false, false, 1),
    queryBuilder(false, false, true, 1)
  )

}

class PlaceSearchRequestHandler(val config: Config, serverContext: SearchContext) extends Actor with Configurable with Logging {
  import PlaceSearchRequestHandler._
  private val esClient: Client = serverContext.esClient
  private var w = emptyStringArray
  private var cats = Seq[String]()
  private var kwids: Array[String] = emptyStringArray
  private var areaSlugs: String = ""

  private def buildFilter(searchParams: SearchParams): FilterBuilder = {
    import searchParams.filters._
    import searchParams.geo._
    import searchParams.idx._
    import searchParams.limits._
    import searchParams.view._


    // filters
    val finalFilter = andFilter(boolFilter.mustNot(termFilter("DeleteFlag", 1l).cache(false))).cache(false)
    val cityFilter = boolFilter.cache(false)
    if (id != "" || !kwids.isEmpty) {
      finalFilter.add(idsFilter(esType).addIds(id.split( """,""").map(_.trim.toUpperCase) ++ kwids: _*))
    }
    if(userid != 0) {
      finalFilter.add(termFilter("UserID", userid).cache(false))
    }
    if(locid != "") {
      finalFilter.add(termsFilter("EDMSLocationID", locid.split(""",""").map(_.trim.toInt) :_*).cache(false))
    }

    if (pin != "") {
      finalFilter.add(termsFilter("PinCode", pin.split( """,""").map(_.trim): _*).cache(false))
    }

    val locFilter = boolFilter.cache(false)
    val analyzedAreas = scala.collection.mutable.Set[String]()
    if (area != "") {
      val areas = area.split( """,""").map(_.trim.toLowerCase)
      areas.foreach { a =>

        val areaWords = analyze(esClient, index, "Area", a)
        val terms = areaWords
          .map(fuzzyQuery("Area", _).prefixLength(1).fuzziness(Fuzziness.TWO))
          .map(spanMultiTermQueryBuilder)
        val areaSpan = spanNearQuery.slop(1).inOrder(false)
        terms.foreach(areaSpan.clause)
        locFilter.should(queryFilter(areaSpan).cache(false))

        val synTerms = areaWords
          .map(fuzzyQuery("AreaSynonyms", _).prefixLength(1).fuzziness(Fuzziness.TWO))
          .map(spanMultiTermQueryBuilder)
        val synAreaSpan = spanNearQuery.slop(1).inOrder(false)
        synTerms.foreach(synAreaSpan.clause)
        locFilter.should(queryFilter(synAreaSpan).cache(false))
        analyzedAreas += areaWords.mkString("-")
      }

      areas.map(a => termFilter("City", a).cache(false)).foreach(locFilter.should)
      areas.map(a => termFilter("CitySynonyms", a).cache(false)).foreach(locFilter.should)
      areas.map(a => termFilter("AreaSlug", a).cache(false)).foreach(locFilter.should)

    }
    areaSlugs = analyzedAreas.mkString("#")

    if (lat != 0.0d || lon != 0.0d)
      locFilter.should(
        geoDistanceRangeFilter("LatLong").cache(true)
          .point(lat, lon)
          .from((if (area == "") fromkm else 0.0d) + "km")
          .to((if (area == "") tokm else 10.0d) + "km")
          .optimizeBbox("indexed")
          .geoDistance(GeoDistance.SLOPPY_ARC))

    if (city != "") {
      val cityParams = city.split( """,""").map(_.trim.toLowerCase)
      cityFilter.should(termsFilter("City", cityParams: _*).cache(false))
      cityFilter.should(termsFilter("CitySynonyms", cityParams: _*).cache(false))
      cityFilter.should(termsFilter("CitySlug", cityParams: _*).cache(false))

      finalFilter.add(cityFilter)
    }

    if (category != "") {
      val cats = category.split("""#""")
      val b = boolFilter.cache(false)
      cats.foreach { c =>
        b.should(queryFilter(matchPhraseQuery("Product.l3category", c).slop(1)).cache(true))
        b.should(termFilter("Product.l3categoryslug", c).cache(false))
      }
      finalFilter.add(nestedFilter("Product", b).cache(false))
    } else if(w.length > 0) {
      val matchedCats = categoryFilter(w, cityFilter, aggbuckets, esClient, index, esType, Math.min(maxdocspershard, int("max-docs-per-shard")))
      if(matchedCats.filter!=null)
        finalFilter.add(matchedCats.filter)
      cats = matchedCats.cats
    }

    if (locFilter.hasClauses) {
      finalFilter.add(locFilter)
    }
    finalFilter
  }

  private def buildSearch(searchParams: SearchParams): SearchRequestBuilder = {
    import searchParams.geo._
    import searchParams.idx._
    import searchParams.limits._
    import searchParams.page._
    import searchParams.text._
    import searchParams.view._

    val sort = (if(lat != 0.0d || lon !=0.0d) "_distance," else "") + "_ct,_mc," + "_score"
    val sorters = getSort(sort, lat, lon, areaSlugs)

    val search = esClient.prepareSearch(index.split(","): _*).setQueryCache(false)
      .setTypes(esType.split(","): _*)
      .setSearchType(SearchType.fromString(searchType))
      .setTrackScores(true)
      .addFields(select.split( ""","""): _*)
      .setFrom(offset).setSize(size)
      .setTimeout(TimeValue.timeValueMillis(Math.min(timeoutms, long("timeoutms"))))
      .setTerminateAfter(Math.min(maxdocspershard, int("max-docs-per-shard")))
      .setExplain(explain)
      .setFetchSource(source)
      .addSorts(sorters)

    if(collapse) {
      val orders: List[Terms.Order] = (
        (if (lat != 0.0d || lon != 0.0d) Some(Terms.Order.aggregation("geo", true)) else None) ::
          Some(Terms.Order.aggregation("customertype", true)) ::
          Some(Terms.Order.aggregation("mediacount", false)) ::
          Some(Terms.Order.aggregation("score", false)) ::
          Nil
        ).flatten

      val order = if(orders.size==1) orders(0) else Terms.Order.compound(orders)

      val collapsed = terms("collapsed").field("MasterID").order(order).size(offset+size)
        .subAggregation(topHits("hits").setFetchSource(source).setSize(1).setExplain(explain).setTrackScores(true).addSorts(sorters))
        .subAggregation(max("score").script("docscore").lang("native"))

      if(lat != 0.0d || lon !=0.0d) {
        collapsed.subAggregation(min("geo").script("geobucket").lang("native").param("lat", lat).param("lon", lon).param("areaSlugs", areaSlugs))
      }
      collapsed.subAggregation(max("mediacount").script("mediacountsort").lang("native"))
      collapsed.subAggregation(min("customertype").script("customertype").lang("native"))

      search.addAggregation(collapsed)
    }

    if (agg) {
      if (city == ""||city.contains(""","""))
        search.addAggregation(terms("city").field("CityAggr").size(100))

      //search.addAggregation(terms("pincodes").field("PinCode").size(aggbuckets))
      search.addAggregation(terms("area").field("AreaAggr").size(aggbuckets))
      search.addAggregation(
        terms("categories").field("Product.l3categoryaggr").size(aggbuckets).order(Terms.Order.aggregation("sum_score", false))
          .subAggregation(sum("sum_score").script("docscore").lang("native"))
      )
      /*
      if (lat != 0.0d || lon != 0.0d)
        search.addAggregation(
          geoDistance("geotarget")
            .field("LatLong")
            .lat(lat).lon(lon)
            .distanceType(GeoDistance.SLOPPY_ARC)
            .unit(DistanceUnit.KILOMETERS)
            .addUnboundedTo("within 1.5 kms", 1.5d)
            .addRange("1.5 to 4 kms", 1.5d, 4d)
            .addRange("4 to 8 kms", 4d, 8d)
            .addRange("8 to 30 kms", 8d, 30d)
            .addUnboundedFrom("30 kms and beyond", 30d)
        )
        */
    }


    if (slugFlag) {
      search.addAggregation(nested("products").path("Product")
        .subAggregation(terms("catkw").field("Product.l3categoryaggr").size(aggbuckets*3).order(Terms.Order.aggregation("sum_score", false))
          .subAggregation(terms("kw").field("Product.categorykeywordsaggr").size(100))
          .subAggregation(sum("sum_score").script("docscore").lang("native"))
        )
      )
      search.addAggregation(terms("areasyns").field("AreaAggr").size(aggbuckets)
        .subAggregation(terms("syns").field("AreaSynonymsAggr").size(aggbuckets))
      )
    }
    search
  }


  override def receive = {
    case searchParams: SearchParams =>
      import searchParams.text._
      import searchParams.idx._
      import searchParams.filters._

      kwids = idregex.findAllIn(kw).toArray.map(_.trim.toUpperCase)
      w = if(kwids.length > 0) emptyStringArray else analyze(esClient, index, "CompanyName", kw)
      if(w.isEmpty && kwids.isEmpty && category.trim == "" && id=="" && userid == 0 && locid == "") {
        context.parent ! EmptyResponse("empty search criteria")
      }
      else {
        val query =
          if (w.length > 0) qDefs(0)(searchFields2, fullFields2, w, w.length, math.max(w.length/2, 1))
          else matchAllQuery()
        val isMatchAll = query.isInstanceOf[MatchAllQueryBuilder]

        // filters
        val finalFilter = buildFilter(searchParams)

        val search = buildSearch(searchParams)

        search.setQuery(filteredQuery(query, finalFilter))

        val me = context.self

        search.execute(new ActionListener[SearchResponse] {
          override def onResponse(response: SearchResponse): Unit = {
            if(response.getHits.totalHits() > 0 || isMatchAll)
              me ! WrappedResponse(searchParams, response, 0)
            else
              me ! ReSearch(searchParams, finalFilter, search, 1, response)
          }

          override def onFailure(e: Throwable): Unit = {
            throw e
          }
        })
      }

    case ReSearch(searchParams, filter, search, relaxLevel, response) =>
      if(relaxLevel >= qDefs.length)
        context.self ! (WrappedResponse(searchParams, response, relaxLevel - 1))
      else {
        val query = qDefs(relaxLevel)(searchFields2, fullFields2, w, w.length, math.max(w.length / 2, 1))
        val me = context.self

        search.setQuery(filteredQuery(query, filter)).execute(new ActionListener[SearchResponse] {
          override def onResponse(response: SearchResponse): Unit = {
            if (response.getHits.totalHits() > 0 || relaxLevel >= qDefs.length - 1)
              me ! WrappedResponse(searchParams, response, relaxLevel)
            else
              me ! ReSearch(searchParams, filter, search, relaxLevel + 1, response)
          }

          override def onFailure(e: Throwable): Unit = {
            throw e
          }
        })
      }

    case response: WrappedResponse =>
      import response.result
      import response.searchParams.filters._
      import response.searchParams.geo._
      import response.searchParams.idx._
      import response.searchParams.limits._
      import response.searchParams.req._
      import response.searchParams.startTime
      import response.searchParams.text._
      import response.searchParams.view._
      import response.relaxLevel

      val areaWords = analyze(esClient, index, "Area", area)


      var slug = ""
      if (slugFlag) {
        val catBucks = result.getAggregations.get("products").asInstanceOf[Nested].getAggregations.get("catkw").asInstanceOf[Terms].getBuckets
        val matchedCat = catBucks
          .find(b => matchAnalyzed(esClient, index, "Product.l3category", b.getKey, w)
          || b.getAggregations.get("kw").asInstanceOf[Terms].getBuckets.exists(c => matchAnalyzed(esClient, index, "Product.categorykeywords", c.getKey, w)))
          .fold("/search/" + urlize(kw))(k => "/" + urlize(k.getKey))
        val areaBucks = result.getAggregations.get("areasyns").asInstanceOf[Terms].getBuckets

        val matchedArea = areaBucks.find(b => matchAnalyzed(esClient, index, "Area", b.getKey, areaWords))
          .fold(//look in synonyms if name not found
            areaBucks.find(b => b.getAggregations.get("syns").asInstanceOf[Terms].getBuckets.exists(
              c => matchAnalyzed(esClient, index, "AreaSynonyms", c.getKey, areaWords))
            ).fold("/in/" + urlize(area))(k => "/in/" + urlize(k.getKey))
          )(k => "/in/" + urlize(k.getKey))

        slug = (if (city != "") "/" + urlize(city) else "") +
          matchedCat +
          (if (category != "") "/cat/" + urlize(category) else "") +
          (if (area != "") matchedArea else "")
      }

      val parsedResult = parse(result.toString).transformField {
        case JField("aggregations", obj: JValue) => JField("aggregations", obj.removeField(_._1=="areasyns").removeField(_._1=="products"))
      }.removeField(_._1=="_shards")


      val endTime = System.currentTimeMillis
      val timeTaken = endTime - startTime
      info("[" + result.getTookInMillis + "/" + timeTaken + (if(result.isTimedOut) " timeout" else "") + "] [" + result.getHits.hits.length + "/" + result.getHits.getTotalHits + (if(result.isTerminatedEarly) " termearly ("+Math.min(maxdocspershard, int("max-docs-per-shard"))+")" else "") + "] [" + clip.toString + "]->[" + httpReq.uri + "]->[" + cats + "]")
      context.parent ! SearchResult(slug, result.getHits.hits.length, timeTaken, relaxLevel, parsedResult)
  }

  def urlize(k: String) =
    URLEncoder.encode(k.replaceAll(pat, " ").trim.replaceAll( """\s+""", "-").toLowerCase, "UTF-8")

}

