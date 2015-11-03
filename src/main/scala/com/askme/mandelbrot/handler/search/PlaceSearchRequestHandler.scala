package com.askme.mandelbrot.handler.search

import java.net.URLEncoder
import java.util

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.EmptyResponse
import com.askme.mandelbrot.handler.message.ErrorResponse
import com.askme.mandelbrot.handler.search.message.{SearchParams, SearchResult}
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
import org.elasticsearch.script.ScriptService.ScriptType
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.aggregations.bucket.nested.Nested
import org.elasticsearch.search.aggregations.bucket.terms.{TermsBuilder, Terms}
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder
import org.elasticsearch.script._
import org.elasticsearch.search.sort._
import org.elasticsearch.search.sort.SortBuilders._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConversions._


/**
 * Created by adichad on 08/01/15.
 */
object PlaceSearchRequestHandler extends Logging {

  val pat = """(?U)[^\p{alnum}]+"""
  val idregex = """[uU]\d+[lL]\d+""".r

  private val randomParams = new util.HashMap[String, AnyRef]
  randomParams.put("buckets", int2Integer(5))

  private def getSort(sort: String, lat: Double = 0d, lon: Double = 0d, areaSlugs: String = "", w: Array[String]) = {
    val parts = for (x <- sort.split(",")) yield x.trim
    parts.map {

      case "_random" =>
        scriptSort(new Script("randomizer", ScriptType.INLINE, "native", randomParams), "number").order(SortOrder.ASC)
      case "_name" =>
        val nameParams = new util.HashMap[String, AnyRef]
        nameParams.put("name", w.mkString(" "))
        scriptSort(new Script("exactnamematch", ScriptType.INLINE, "native", nameParams), "number").order(SortOrder.ASC)
      case "_score" => scoreSort.order(SortOrder.DESC)
      case "_distance" =>
        val geoParams = new util.HashMap[String, AnyRef]
        geoParams.put("lat", double2Double(lat))
        geoParams.put("lon", double2Double(lon))
        geoParams.put("areaSlugs", areaSlugs)
        geoParams.put("areafield", "AreaDocVal")
        geoParams.put("synfield", "AreaSynonymsDocVal")
        geoParams.put("skufield", "SKUAreasDocVal")
        scriptSort(new Script("geobucket", ScriptType.INLINE, "native", geoParams), "number").order(SortOrder.ASC)
      case "_tags" =>
        val tagParams = new util.HashMap[String, AnyRef]
        tagParams.put("shingles", (1 to 3).flatMap(w.sliding(_).map(_.mkString(" "))).mkString("#"))
        scriptSort(new Script("curatedtag", ScriptType.INLINE, "native", tagParams), "number").order(SortOrder.DESC)
      case "_ct" => scriptSort(new Script("customertype", ScriptType.INLINE, "native",
        new util.HashMap[String, AnyRef]), "number").order(SortOrder.ASC)
      case "_mc" => scriptSort(new Script("mediacountsort", ScriptType.INLINE, "native",
        new util.HashMap[String, AnyRef]), "number").order(SortOrder.DESC)
      case x =>
        val pair = x.split( """\.""", 2)
        if (pair.size == 2)
          new FieldSortBuilder(pair(0)).order(SortOrder.valueOf(pair(1)))
        else
          new FieldSortBuilder(pair(0)).order(SortOrder.DESC)
    }
  }


  private[PlaceSearchRequestHandler] def nestIfNeeded(fieldName: String, q: QueryBuilder): QueryBuilder = {
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
    val terms: Array[SpanQueryBuilder] = w.map(x=>
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

  private def currQuery(tokenFields: Map[String, Float],
                recomFields: Map[String, Float],
                w: Array[String], fuzzy: Boolean = false, sloppy: Boolean = false, span: Boolean = false, tokenRelax: Int = 0) = {

    if(span)
      disMaxQuery.addAll(tokenFields.map(field => shingleSpan(field._1, field._2, w, 1, w.length, math.max(w.length-tokenRelax, 1), sloppy, fuzzy)))
    else {
      disMaxQuery.addAll(recomFields.map(field =>
        if(field._1=="LocationNameExact")
          shingleSpan("LocationName", field._2, w, 1, w.length, math.max(w.length-tokenRelax, 1), sloppy, fuzzy)
        else if(field._1=="CompanyAliasesExact")
          shingleSpan("CompanyAliases", field._2, w, 1, w.length, math.max(w.length-tokenRelax, 1), sloppy, fuzzy)
        else if(field._1=="product_l3categoryexact")
          shingleSpan("product_l3category", field._2, w, 1, w.length, math.max(w.length-tokenRelax, 1), sloppy, fuzzy)
        else if(field._1=="product_l2categoryexact")
          shingleSpan("product_l2category", field._2, w, 1, w.length, math.max(w.length-tokenRelax, 1), sloppy, fuzzy)
        else if(field._1=="Product.l1categoryexact")
          shingleSpan("product_l1category", field._2, w, 1, w.length, math.max(w.length-tokenRelax, 1), sloppy, fuzzy)
        else
          shingleFull(field._1, field._2, w, 1, w.length, math.max(w.length - tokenRelax, 1), fuzzy))
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

  private def analyze(esClient: Client, index: String, field: String, text: String): Array[String] =
    new AnalyzeRequestBuilder(esClient.admin.indices, AnalyzeAction.INSTANCE, index, text).setField(field).get().getTokens.map(_.getTerm).toArray


  private val searchFields2 = Map("LocationName" -> 1000000000f, "CompanyAliases" -> 1000000000f,
    "Product_l3category" -> 10000000f,
    "Product_l2category" -> 1000f,
    "Product_l1category" -> 100f,
    "LocationType"->1000f,
    "BusinessType"->1000f,
    "product_name" -> 1000f,
    "product_brand" -> 10000f,
    "CuratedTags"-> 10000f,
    "product_categorykeywords" -> 10000000f,
    "product_parkedkeywords" -> 10000000f,
    "product_stringattribute_answer" -> 100f,
    "Area"->10f, "AreaSynonyms"->10f,
    "City"->1f, "CitySynonyms"->1f,"PinCode"->1f,"Address"->1f)

  private val fullFields2 = Map(
    "LocationNameExact"->100000000000f, "CompanyAliasesExact"->100000000000f,
    "product_l3categoryexact"->10000000000f,
    "product_l2categoryexact"->10000000f,
    "product_l1categoryexact"->10000000f,
    "LocationTypeExact"->1000f,
    "BusinessTypeExact"->1000f,
    "product_nameexact" -> 1000f,
    "product_brandexact" -> 10000f,
    "CuratedTagsExact"-> 10000f,
    "product_categorykeywordsexact"->10000000000f,
    "product_parkedkeywordsexact"->10000000000f,
    "product_stringattribute_answerexact"->100000f,
    "AreaExact"->10f, "AreaSynonymsExact"->10f,
    "City"->1f, "CitySynonyms"->1f,"PinCode"->1f,"AddressExact"->1f)


  private val emptyStringArray = new Array[String](0)

  private def superBoost(len: Int) = math.pow(10, math.min(10,len+1)).toFloat

  private case class WrappedResponse(searchParams: SearchParams, result: SearchResponse, relaxLevel: Int)
  private case class ReSearch(searchParams: SearchParams, filter: BoolQueryBuilder, search: SearchRequestBuilder, relaxLevel: Int, response: SearchResponse)

  private def queryBuilder(tokenFields: Map[String, Float], recomFields: Map[String, Float], fuzzy: Boolean = false, sloppy: Boolean = false, span: Boolean = false, minShingle: Int = 1, tokenRelax: Int = 0)
                          (w: Array[String], maxShingle: Int) = {
    shinglePartition(tokenFields, recomFields, w, maxShingle, minShingle, fuzzy, sloppy, span, tokenRelax)
  }

  private val qDefs: Seq[((Array[String], Int)=>QueryBuilder, Int)] = Seq(

    (queryBuilder(searchFields2, fullFields2, fuzzy = false, sloppy = false, span = false, 1, 0), 1), //1
    // full-shingle exact full matches

    (queryBuilder(searchFields2, fullFields2, fuzzy = true, sloppy = false, span = false, 1, 0), 1), //3
    // full-shingle fuzzy full matches

    //(queryBuilder(searchFields2, fullFields2, false, false, true, 2, 0), 1), //1
    // full-shingle exact span matches

    (queryBuilder(searchFields2, fullFields2, fuzzy = false, sloppy = true, span = true, 1, 0), 1), //5
    // full-shingle exact sloppy-span matches

    (queryBuilder(searchFields2, fullFields2, fuzzy = false, sloppy = false, span = false, 1, 1), 1), //6
    // relaxed-shingle exact full matches

    (queryBuilder(searchFields2, fullFields2, fuzzy = false, sloppy = false, span = true, 2, 1), 1)//7
    // relaxed-shingle exact span matches

    //(queryBuilder(searchFields2, fullFields2, false, false, true, 2, 2), 1) //7
    // relaxed-shingle exact span matches


  )

}

class PlaceSearchRequestHandler(val config: Config, serverContext: SearchContext) extends Actor with Configurable with Logging {
  import PlaceSearchRequestHandler._
  private val esClient: Client = serverContext.esClient
  private var w = emptyStringArray
  private var kwids: Array[String] = emptyStringArray
  private var areaSlugs: String = ""

  private def buildFilter(searchParams: SearchParams): BoolQueryBuilder = {
    import searchParams.filters._
    import searchParams.geo._
    import searchParams.idx._


    // filters
    val finalFilter = boolQuery()

    if (id != "" || !kwids.isEmpty) {
      finalFilter.must(
        boolQuery()
          .should(termQuery("DeleteFlag", 0l))
          .should(
            boolQuery()
              .must(existsQuery("MergedToID"))
              .mustNot(termQuery("MergedToID",""))))
      finalFilter.must(idsQuery(esType).addIds(id.split( """,""").map(_.trim.toUpperCase) ++ kwids: _*))
    } else {
      finalFilter.mustNot(termQuery("DeleteFlag", 1l))
    }
    if(userid != 0) {
      finalFilter.must(termQuery("UserID", userid))
    }
    if(locid != "") {
      finalFilter.must(termsQuery("EDMSLocationID", locid.split(""",""").map(_.trim.toInt) :_*))
    }

    if (pin != "") {
      finalFilter.must(termsQuery("PinCode", pin.split( """,""").map(_.trim): _*))
    }

    val locFilter = boolQuery
    if (area != "") {
      val areas: Array[String] = area.split(""",""").map(analyze(esClient, index, "AreaExact", _).mkString(" ")).filter(!_.isEmpty)
      areas.map(fuzzyOrTermQuery("AreaExact", _, 1f, 1, fuzzy = true)).foreach(a => locFilter should a)
      areas.map(fuzzyOrTermQuery("AreaSynonymsExact", _, 1f, 1, fuzzy = true)).foreach(a => locFilter should a)
      areas.map(fuzzyOrTermQuery("City", _, 1f, 1, fuzzy = true)).foreach(a => locFilter should a)
      areas.map(fuzzyOrTermQuery("CitySynonyms", _, 1f, 1, fuzzy = true)).foreach(a => locFilter should a)
      areas.map(fuzzyOrTermQuery("SKUAreas", _, 1f, 1, fuzzy = true)).foreach(a => locFilter should a)
      areaSlugs = areas.mkString("#")
    }

    if (lat != 0.0d || lon != 0.0d)
      locFilter.should(
        geoDistanceRangeQuery("LatLong")
          .point(lat, lon)
          .from((if (area == "") fromkm else 0.0d) + "km")
          .to((if (area == "") tokm else 10.0d) + "km")
          .optimizeBbox("indexed")
          .geoDistance(GeoDistance.SLOPPY_ARC))


    if (locFilter.hasClauses) {
      finalFilter.must(locFilter)
    }

    if (city != "") {
      val cityFilter = boolQuery
      city.split( """,""").map(analyze(esClient, index, "City", _).mkString(" ")).filter(!_.isEmpty).foreach { c =>
        cityFilter.should(termQuery("City", c))
        cityFilter.should(termQuery("CitySynonyms", c))
      }

      if(cityFilter.hasClauses)
        finalFilter.should(cityFilter)
    }

    if (category != "") {
      val b = boolQuery
      category.split("""#""").map(analyze(esClient, index, "product_l3categoryexact", _).mkString(" ")).filter(!_.isEmpty).foreach { c =>
        val cat = analyze(esClient, index, "product_l3categoryexact", c).mkString(" ")
        b.should(termQuery("product_l3categoryexact", cat))
        b.should(termQuery("product_categorykeywordsexact", cat))
      }
      if(b.hasClauses)
        finalFilter.should(nestedQuery("Product", b))
    }

    finalFilter
  }

  private def buildSearch(searchParams: SearchParams): SearchRequestBuilder = {
    import searchParams.geo._
    import searchParams.idx._
    import searchParams.limits._
    import searchParams.page._
    import searchParams.view._

    //val sort = if(lat != 0.0d || lon !=0.0d) "_name,_distance,_ct,_mc,_score" else "_ct,_name,_mc,_score"
    val sort =
      (if(lat != 0.0d || lon !=0.0d || areaSlugs.nonEmpty) "_distance," else "") +
        (if(!goldcollapse)"_ct," else "") +
        "_name,_tags,"+"_mc," + "_score"
    val sorters = getSort(sort, lat, lon, areaSlugs, w)

    val search: SearchRequestBuilder = esClient.prepareSearch(index.split(","): _*)
      .setTypes(esType.split(","): _*)
      .setTrackScores(true)
      .setTimeout(TimeValue.timeValueMillis(Math.min(timeoutms, long("timeoutms"))))
      .setTerminateAfter(Math.min(maxdocspershard, int("max-docs-per-shard")))
      .setExplain(explain)
      .setSearchType(SearchType.fromString(searchType, ParseFieldMatcher.STRICT))
      .addSorts(sorters)
      .setFrom(offset).setSize(size)

    if(version<=1) {
      search.setFetchSource(source)
      search.addFields(select.split(""","""):_*)
    } else {
      search.setFetchSource(select.split(""","""), unselect.split(""","""))
    }

    if(collapse) {
      val orders: List[Terms.Order] = (
        Some(Terms.Order.aggregation("exactname", true)) ::
          (if (lat != 0.0d || lon != 0.0d || areaSlugs.nonEmpty) Some(Terms.Order.aggregation("geo", true)) else None) ::
          (if(goldcollapse)Some(Terms.Order.aggregation("random", true)) else None) ::
          Some(Terms.Order.aggregation("tags", false)) ::
          Some(Terms.Order.aggregation("mediacount", false)) ::
          Some(Terms.Order.aggregation("score", false)) ::
          Nil
        ).flatten

      val order = if(orders.size==1) orders.head else Terms.Order.compound(orders)

      val platinum: TermsBuilder = terms("masters").field("MasterID").order(order).size(10)
        .subAggregation(topHits("hits").setFetchSource(select.split(""","""), unselect.split(""",""")).setSize(1).setExplain(explain).setTrackScores(true).addSorts(sorters))
      val diamond = terms("masters").field("MasterID").order(order).size(15)
        .subAggregation(topHits("hits").setFetchSource(select.split(""","""), unselect.split(""",""")).setSize(1).setExplain(explain).setTrackScores(true).addSorts(sorters))
      val gold = terms("masters").field("MasterID").order(order).size(25)
        .subAggregation(topHits("hits").setFetchSource(select.split(""","""), unselect.split(""",""")).setSize(1).setExplain(explain).setTrackScores(true).addSorts(sorters))


      if(goldcollapse) {
        val randomParams = new util.HashMap[String, AnyRef]
        randomParams.put("buckets", int2Integer(5))
        val randomizer = min("random").script(new Script("randomizer", ScriptType.INLINE, "native", randomParams))
        platinum.subAggregation(randomizer)
        diamond.subAggregation(randomizer)
        gold.subAggregation(randomizer)
      }


      val nameParams = new util.HashMap[String, AnyRef]
      nameParams.put("name", w.mkString(" "))
      val nameSorter = min("exactname").script(new Script("exactnamematch", ScriptType.INLINE, "native", nameParams))
      platinum.subAggregation(nameSorter)
      diamond.subAggregation(nameSorter)
      gold.subAggregation(nameSorter)
      if(lat != 0.0d || lon !=0.0d || areaSlugs.nonEmpty) {
        val geoParams = new util.HashMap[String, AnyRef]
        geoParams.put("lat", double2Double(lat))
        geoParams.put("lon", double2Double(lon))
        geoParams.put("areaSlugs", areaSlugs)
        val geoSorter = min("geo").script(new Script("geobucket", ScriptType.INLINE, "native", geoParams))
        platinum.subAggregation(geoSorter)
        diamond.subAggregation(geoSorter)
        gold.subAggregation(geoSorter)
      }

      val tagsParams = new util.HashMap[String, AnyRef]
      tagsParams.put("shingles", (1 to 3).flatMap(w.sliding(_).map(_.mkString(" "))).mkString("#"))
      val tagSorter = max("tags").script(new Script("curatedtag", ScriptType.INLINE, "native", tagsParams))

      val mediaSorter = max("mediacount").script(new Script("mediacountsort", ScriptType.INLINE, "native", new util.HashMap[String, AnyRef]))
      val scoreSorter = max("score").script(new Script("docscore", ScriptType.INLINE, "native", new util.HashMap[String, AnyRef]))
      platinum.subAggregation(tagSorter)
      platinum.subAggregation(mediaSorter)
      platinum.subAggregation(scoreSorter)

      diamond.subAggregation(tagSorter)
      diamond.subAggregation(mediaSorter)
      diamond.subAggregation(scoreSorter)

      gold.subAggregation(tagSorter)
      gold.subAggregation(mediaSorter)
      gold.subAggregation(scoreSorter)

      val platinumFilter = if (area != "") {
        val areaFilter = boolQuery
        area.split(""",""").map(analyze(esClient, index, "SKUAreas", _).mkString(" ")).filter(!_.isEmpty)
          .map(fuzzyOrTermQuery("SKUAreas", _, 1f, 1, fuzzy = true)).foreach(a => areaFilter should a)
        boolQuery.must(areaFilter).must(termQuery("CustomerType", "550")).must(existsQuery("SKUAreasDocVal"))
      } else {
        boolQuery.must(termQuery("CustomerType", "550")).must(existsQuery("SKUAreasDocVal"))
      }


      val diamondFilter = if (area != "") {
        val areaFilter = boolQuery
        area.split(""",""").map(analyze(esClient, index, "SKUAreas", _).mkString(" ")).filter(!_.isEmpty)
          .map(fuzzyOrTermQuery("SKUAreas", _, 1f, 1, fuzzy = true)).foreach(a => areaFilter should a)
        boolQuery.must(areaFilter).must(termQuery("CustomerType", "450")).must(existsQuery("SKUAreasDocVal"))
      } else {
        boolQuery.must(termQuery("CustomerType", "450")).must(existsQuery("SKUAreasDocVal"))
      }

      val goldFilter = termQuery("CustomerType", "350")

      search.addAggregation(filter("platinum").filter(platinumFilter).subAggregation(platinum))
      search.addAggregation(filter("diamond").filter(diamondFilter).subAggregation(diamond))
      if(goldcollapse)
        search.addAggregation(filter("gold").filter(goldFilter).subAggregation(gold))


    }

    if (agg) {
      if (city == ""||city.contains(""","""))
        search.addAggregation(terms("city").field("CityAggr").size(100))

      //search.addAggregation(terms("pincodes").field("PinCode").size(aggbuckets))
      search.addAggregation(terms("area").field("AreaAggr").size(aggbuckets))
      search.addAggregation(
        terms("categories").field("product_l3categoryaggr").size(aggbuckets).order(Terms.Order.aggregation("sum_score", false))
          .subAggregation(sum("sum_score").script(new Script("docscore", ScriptType.INLINE, "native", new util.HashMap[String, AnyRef])))
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
          .subAggregation(sum("sum_score").script(new Script("docscore", ScriptType.INLINE, "native", new util.HashMap[String, AnyRef])))
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
        try {
          import searchParams.text._
          import searchParams.idx._
          import searchParams.filters._
          import searchParams.startTime
          import searchParams.req._

          kwids = idregex.findAllIn(kw).toArray.map(_.trim.toUpperCase)
          w = if (kwids.length > 0) emptyStringArray else analyze(esClient, index, "CompanyName", kw)
          if (w.length>12) w = emptyStringArray
          w = w.take(8)
          if (w.isEmpty && kwids.isEmpty && category.trim == "" && id == "" && userid == 0 && locid == "") {
            context.parent ! EmptyResponse("empty search criteria")
          }
          else {
            val query =
              if (w.length > 0) qDefs.head._1(w, w.length)
              else matchAllQuery()
            val leastCount = qDefs.head._2
            val isMatchAll = query.isInstanceOf[MatchAllQueryBuilder]

            // filters
            val finalFilter = buildFilter(searchParams)

            val search = buildSearch(searchParams)

            search.setQuery(boolQuery.must(query).filter(finalFilter))

            val me = context.self

            search.execute(new ActionListener[SearchResponse] {
              override def onResponse(response: SearchResponse): Unit = {
                if (response.getHits.totalHits() >= leastCount || isMatchAll)
                  me ! WrappedResponse(searchParams, response, 0)
                else
                  me ! ReSearch(searchParams, finalFilter, search, 1, response)
              }

              override def onFailure(e: Throwable): Unit = {
                val timeTaken = System.currentTimeMillis() - startTime
                error("[" + timeTaken + "] [q0] [na/na] [" + clip.toString + "]->[" + httpReq.uri + "]->[]")
                context.parent ! ErrorResponse(e.getMessage, e)
              }
            })
          }
        } catch {
          case e: Throwable =>
            context.parent ! ErrorResponse(e.getMessage, e)
        }

      case ReSearch(searchParams, filter, search, relaxLevel, response) =>
        try {
          import searchParams.startTime
          import searchParams.req._

          if (relaxLevel >= qDefs.length)
            context.self ! WrappedResponse(searchParams, response, relaxLevel - 1)
          else {
            val query = qDefs(relaxLevel)._1(w, w.length)
            val leastCount = qDefs(relaxLevel)._2
            val me = context.self

            search.setQuery(boolQuery.must(query).filter(filter)).execute(new ActionListener[SearchResponse] {
              override def onResponse(response: SearchResponse): Unit = {
                if (response.getHits.totalHits() >= leastCount || relaxLevel >= qDefs.length - 1)
                  me ! WrappedResponse(searchParams, response, relaxLevel)
                else
                  me ! ReSearch(searchParams, filter, search, relaxLevel + 1, response)
              }

              override def onFailure(e: Throwable): Unit = {
                val timeTaken = System.currentTimeMillis() - startTime
                error("[" + timeTaken + "] [q" + relaxLevel + "] [na/na] [" + clip.toString + "]->[" + httpReq.uri + "]->[]")
                context.parent ! ErrorResponse(e.getMessage, e)
              }
            })
          }
        } catch {
          case e: Throwable =>
            context.parent ! ErrorResponse(e.getMessage, e)
        }

      case response: WrappedResponse =>
        try {
          import response.result
          import response.searchParams.filters._
          import response.searchParams.geo._
          import response.searchParams.idx._
          import response.searchParams.limits._
          import response.searchParams.req._
          import response.searchParams.startTime
          import response.searchParams.view._
          import response.relaxLevel

          val areaWords = analyze(esClient, index, "Area", area)


          var slug = ""
          if (slugFlag) {
            val catBucks = result.getAggregations.get("products").asInstanceOf[Nested].getAggregations.get("catkw").asInstanceOf[Terms].getBuckets
            val matchedCat = catBucks
              .find(b => matchAnalyzed(esClient, index, "Product.l3category", b.getKeyAsString, w)
              || b.getAggregations.get("kw").asInstanceOf[Terms].getBuckets.exists(c => matchAnalyzed(esClient, index, "Product.categorykeywords", c.getKeyAsString, w)))
              .fold("/search/" + urlize(w.mkString(" ")))(k => "/" + urlize(analyze(esClient, index, "Product.l3categoryexact", k.getKeyAsString).mkString(" ")))
            val areaBucks = result.getAggregations.get("areasyns").asInstanceOf[Terms].getBuckets

            val matchedArea = areaBucks.find(b => matchAnalyzed(esClient, index, "Area", b.getKeyAsString, areaWords))
              .fold(//look in synonyms if name not found
                areaBucks.find(b => b.getAggregations.get("syns").asInstanceOf[Terms].getBuckets.exists(
                  c => matchAnalyzed(esClient, index, "AreaSynonyms", c.getKeyAsString, areaWords))
                ).fold("/in/" + urlize(area))(k => "/in/" + urlize(k.getKeyAsString))
              )(k => "/in/" + urlize(k.getKeyAsString))

            slug = (if (city != "") "/" + urlize(city) else "") +
              matchedCat +
              (if (category != "") "/cat/" + urlize(category) else "") +
              (if (area != "") matchedArea else "")
          }

          val cats = if (agg) result.getAggregations.get("categories").asInstanceOf[Terms].getBuckets.map(_.getKey).mkString(", ") else ""

          val parsedResult = parse(result.toString)
          /*.transformField {
            case JField("aggregations", obj: JValue) => JField("aggregations", obj.removeField(_._1=="areasyns").removeField(_._1=="products"))
          }.removeField(_._1=="_shards")*/


          val endTime = System.currentTimeMillis
          val timeTaken = endTime - startTime
          info("[" + result.getTookInMillis + "/" + timeTaken + (if (result.isTimedOut) " timeout" else "") + "] [q" + relaxLevel + "] [" + result.getHits.hits.length + "/" + result.getHits.getTotalHits + (if (result.isTerminatedEarly) " termearly (" + Math.min(maxdocspershard, int("max-docs-per-shard")) + ")" else "") + "] [" + clip.toString + "]->[" + httpReq.uri + "]->[" + cats + "]")
          context.parent ! SearchResult(slug, result.getHits.hits.length, timeTaken, relaxLevel, parsedResult)
        } catch {
          case e: Throwable =>
            context.parent ! ErrorResponse(e.getMessage, e)
        }

  }

  def urlize(k: String) =
    URLEncoder.encode(k.replaceAll(pat, " ").trim.replaceAll( """\s+""", "-").toLowerCase, "UTF-8")

}

