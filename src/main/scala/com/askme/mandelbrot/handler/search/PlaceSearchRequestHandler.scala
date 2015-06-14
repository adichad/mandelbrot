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
import org.elasticsearch.client.Client
import org.elasticsearch.common.geo.GeoDistance
import org.elasticsearch.common.unit.{Fuzziness, TimeValue}
import org.elasticsearch.index.query.FilterBuilders._
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.index.query._
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.aggregations.bucket.nested.Nested
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder
import org.elasticsearch.search.sort._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConversions._


/**
 * Created by adichad on 08/01/15.
 */


object PlaceSearchRequestHandler extends Logging {

  val pat = """(?U)[^\p{alnum}]+"""
  val idregex = """[uU]\d+[lL]\d+""".r

  private def getSort(sort: String, lat: Double = 0d, lon: Double = 0d, areaSlugs: String = "", w: Array[String]) = {
    val parts = for (x <- sort.split(",")) yield x.trim
    parts.map(
      _ match {
        case "_name" => SortBuilders.scriptSort("exactnamematch", "number").lang("native").order(SortOrder.ASC).
          param("name", w.mkString(" "))
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
      disMaxQuery.addAll(tokenFields.map(field => shingleSpan(field._1, field._2, w, 1, w.length, math.max(w.length-tokenRelax, 1), sloppy, fuzzy)))
    else
      disMaxQuery.addAll(recomFields.map(field => shingleFull(field._1, field._2, w, 1, w.length, math.max(w.length-tokenRelax, 1), fuzzy)))
  }

  private def shinglePartition(tokenFields: Map[String, Float], recomFields: Map[String, Float], w: Array[String],
                               maxShingle: Int, minShingle: Int = 1, fuzzy: Boolean = false, sloppy: Boolean = false,
                               span: Boolean = false, tokenRelax: Int = 0): BoolQueryBuilder = {

    if(w.length>0)
      boolQuery.minimumNumberShouldMatch(1).shouldAll(
        (math.max(1, math.min(minShingle, w.length)) to math.min(maxShingle, w.length)).map(len=>(w.slice(0, len), w.slice(len, w.length))).map { x =>
          //info(x._1.toList.toString+","+x._2.toList.toString)
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
    new AnalyzeRequestBuilder(esClient.admin.indices, index, text).setField(field).get().getTokens.map(_.getTerm).toArray

  private val searchFieldsName = Map(
    "LocationName" -> 1000000000f, "CompanyAliases" -> 1000000000f,
    "Area"->10f, "AreaSynonyms"->10f,
    "City"->1f, "CitySynonyms"->1f)


  private val fullFieldsName = Map(
    "LocationNameExact"->100000000000f, "CompanyAliasesExact"->100000000000f,
    "AreaExact"->10f, "AreaSynonymsExact"->10f,
    "City"->1f, "CitySynonyms"->1f)

  private val searchFields = Map("LocationName" -> 1000000000f, "CompanyAliases" -> 1000000000f,
    "Product.l3category" -> 10000000f,
    "Product.l2category" -> 1000f,
    "Product.l1category" -> 100f,
    "LocationType"->1000f,
    "BusinessType"->1000f,
    "Product.name" -> 1000f,
    "Product.brand" -> 10000f,
    "Product.categorykeywords" -> 10000000f,
    "Area"->10f, "AreaSynonyms"->10f,
    "City"->1f, "CitySynonyms"->1f)

  private val fullFields = Map(
    "LocationNameExact"->100000000000f, "CompanyAliasesExact"->100000000000f,
    "Product.l3categoryexact"->10000000000f,
    "Product.l2categoryexact"->10000000f,
    "Product.l1categoryexact"->10000000f,
    "LocationTypeExact"->1000f,
    "BusinessTypeExact"->1000f,
    "Product.nameexact" -> 1000f,
    "Product.brandexact" -> 10000f,
    "Product.categorykeywordsexact"->10000000000f,
    "AreaExact"->10f, "AreaSynonymsExact"->10f,
    "City"->1f, "CitySynonyms"->1f)

  private val searchFields2 = Map("LocationName" -> 1000000000f, "CompanyAliases" -> 1000000000f,
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

  private val fullFields2 = Map(
    "LocationNameExact"->100000000000f, "CompanyAliasesExact"->100000000000f,
    "Product.l3categoryexact"->10000000000f,
    "Product.l2categoryexact"->10000000f,
    "Product.l1categoryexact"->10000000f,
    "LocationTypeExact"->1000f,
    "BusinessTypeExact"->1000f,
    "Product.nameexact" -> 1000f,
    "Product.brandexact" -> 10000f,
    "Product.categorykeywordsexact"->10000000000f,
    "Product.stringattribute.answerexact"->100000f,
    "AreaExact"->10f, "AreaSynonymsExact"->10f,
    "City"->1f, "CitySynonyms"->1f)


  private val emptyStringArray = new Array[String](0)

  private def superBoost(len: Int) = math.pow(10, math.min(10,len+1)).toFloat

  private case class WrappedResponse(searchParams: SearchParams, result: SearchResponse, relaxLevel: Int)
  private case class ReSearch(searchParams: SearchParams, filter: FilterBuilder, search: SearchRequestBuilder, relaxLevel: Int, response: SearchResponse)

  private def queryBuilder(tokenFields: Map[String, Float], recomFields: Map[String, Float], fuzzy: Boolean = false, sloppy: Boolean = false, span: Boolean = false, minShingle: Int = 1, tokenRelax: Int = 0)
                          (w: Array[String], maxShingle: Int) = {
    shinglePartition(tokenFields, recomFields, w, maxShingle, minShingle, fuzzy, sloppy, span, tokenRelax)
  }

  private val qDefs: Seq[((Array[String], Int)=>BaseQueryBuilder, Int)] = Seq(
    //                                      fuzzy, slop,  span, minshingle, tokenrelax
    (queryBuilder(searchFieldsName, fullFieldsName, false, false, false, 1, 0), 3), //0
    // full-shingle exact full matches

    (queryBuilder(searchFieldsName, fullFieldsName, true, false, false, 1, 0), 3), //2
    // full-shingle fuzzy full matches

    (queryBuilder(searchFieldsName, fullFieldsName, false, false, false, 1, 1), 3), //3
    // relaxed-shingle exact full matches


    //                                      fuzzy, slop,  span, minshingle, tokenrelax
    (queryBuilder(searchFields, fullFields, false, false, false, 1, 0), 3), //0
    // full-shingle exact full matches

    (queryBuilder(searchFields, fullFields, false, false, true, 2, 0), 3), //1
    // full-shingle exact span matches

    (queryBuilder(searchFields, fullFields, true, false, false, 1, 0), 5), //2
    // full-shingle fuzzy full matches

    (queryBuilder(searchFields, fullFields, false, false, false, 1, 1), 3), //3
    // relaxed-shingle exact full matches


    //                                        fuzzy, slop,  span, minshingle, tokenrelax
    (queryBuilder(searchFields2, fullFields2, false, false, false, 1, 0), 3), //4
    // full-shingle exact full matches

    (queryBuilder(searchFields2, fullFields2, false, false, true, 2, 0), 3), //5
    // full-shingle exact span matches

    (queryBuilder(searchFields2, fullFields2, true, false, false, 1, 0), 5), //6
    // full-shingle fuzzy full matches

    (queryBuilder(searchFields2, fullFields2, false, false, false, 1, 1), 3), //7
    // relaxed-shingle exact full matches

    (queryBuilder(searchFields, fullFields, false, false, true, 2, 1), 3), //8
    // relaxed-shingle exact span matches

    (queryBuilder(searchFields2, fullFields2, false, false, true, 2, 1), 3), //9
    // relaxed-shingle exact span matches

    (queryBuilder(searchFields, fullFields, true, false, true, 2, 0), 5), //10
    // full-shingle fuzzy span matches

    (queryBuilder(searchFields, fullFields, false, true, true, 2, 0), 3), //11
    // full-shingle exact sloppy-span matches

    (queryBuilder(searchFields, fullFields, false, true, true, 2, 1), 3), //12
    // relaxed-shingle exact sloppy-span matches

    (queryBuilder(searchFields2, fullFields2, true, false, true, 2, 0), 5), //13
    // full-shingle fuzzy span matches

    (queryBuilder(searchFields2, fullFields2, false, true, true, 2, 0), 3), //14
    // full-shingle exact sloppy-span matches

    (queryBuilder(searchFields2, fullFields2, false, true, true, 2, 1), 3) //15
    // relaxed-shingle exact sloppy-span matches

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
    if (area != "") {
      val areas = area.split(""",""").map(analyze(esClient, index, "AreaExact", _).mkString(" "))
      areas.map(fuzzyOrTermQuery("AreaExact", _, 1f, 1, true)).foreach(a => locFilter should queryFilter(a).cache(false))
      areas.map(fuzzyOrTermQuery("AreaSynonymsExact", _, 1f, 1, true)).foreach(a => locFilter should queryFilter(a).cache(false))
      areas.map(fuzzyOrTermQuery("City", _, 1f, 1, true)).foreach(a => locFilter should queryFilter(a).cache(false))
      areas.map(fuzzyOrTermQuery("CitySynonyms", _, 1f, 1, true)).foreach(a => locFilter should queryFilter(a).cache(false))
      areaSlugs = areas.map(_.replace(' ', '-')).mkString("#")
    }

    if (lat != 0.0d || lon != 0.0d)
      locFilter.should(
        geoDistanceRangeFilter("LatLong").cache(true)
          .point(lat, lon)
          .from((if (area == "") fromkm else 0.0d) + "km")
          .to((if (area == "") tokm else 10.0d) + "km")
          .optimizeBbox("indexed")
          .geoDistance(GeoDistance.SLOPPY_ARC))


    if (locFilter.hasClauses) {
      finalFilter.add(locFilter)
    }

    if (city != "") {
      val cityParams = city.split( """,""").map(analyze(esClient, index, "City", _).mkString(" "))
      cityFilter.should(termsFilter("City", cityParams: _*).cache(false))
      cityFilter.should(termsFilter("CitySynonyms", cityParams: _*).cache(false))

      finalFilter.add(cityFilter)
    }

    if (category != "") {
      val cats = category.split("""#""")
      val b = boolFilter.cache(false)
      cats.foreach { c =>
        val cat = analyze(esClient, index, "Product.l3categoryexact", c).mkString(" ")
        b.should(termFilter("Product.l3categoryexact", cat)).cache(true)
        b.should(termFilter("Product.categorykeywordsexact", cat)).cache(false)
      }
      finalFilter.add(nestedFilter("Product", b).cache(false))
    }

    finalFilter
  }

  private def buildSearch(searchParams: SearchParams): SearchRequestBuilder = {
    import searchParams.geo._
    import searchParams.idx._
    import searchParams.limits._
    import searchParams.page._
    import searchParams.view._

    val sort = /*"_name," + */(if(lat != 0.0d || lon !=0.0d) "_distance," else "") + "_ct,_mc," + "_score"
    val sorters = getSort(sort, lat, lon, areaSlugs, w)

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
        /*Some(Terms.Order.aggregation("exactname", true)) ::*/
          (if (lat != 0.0d || lon != 0.0d) Some(Terms.Order.aggregation("geo", true)) else None) ::
          Some(Terms.Order.aggregation("customertype", true)) ::
          Some(Terms.Order.aggregation("mediacount", false)) ::
          Some(Terms.Order.aggregation("score", false)) ::
          Nil
        ).flatten

      val order = if(orders.size==1) orders(0) else Terms.Order.compound(orders)

      val collapsed = terms("collapsed").field("MasterID").order(order).size(offset+size)
        .subAggregation(topHits("hits").setFetchSource(source).setSize(1).setExplain(explain).setTrackScores(true).addSorts(sorters))

      //collapsed.subAggregation(min("exactname").script("exactnamematch").lang("native").param("name", w.mkString(" ")))
      if(lat != 0.0d || lon !=0.0d) {
        collapsed.subAggregation(min("geo").script("geobucket").lang("native").param("lat", lat).param("lon", lon).param("areaSlugs", areaSlugs))
      }
      collapsed.subAggregation(min("customertype").script("customertype").lang("native"))
      collapsed.subAggregation(max("mediacount").script("mediacountsort").lang("native"))
      collapsed.subAggregation(max("score").script("docscore").lang("native"))

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
          if (w.length > 0) qDefs(0)._1(w, w.length)
          else matchAllQuery()
        val leastCount = qDefs(0)._2
        val isMatchAll = query.isInstanceOf[MatchAllQueryBuilder]

        // filters
        val finalFilter = buildFilter(searchParams)

        val search = buildSearch(searchParams)

        search.setQuery(filteredQuery(query, finalFilter))

        val me = context.self

        search.execute(new ActionListener[SearchResponse] {
          override def onResponse(response: SearchResponse): Unit = {
            if(response.getHits.totalHits() >= leastCount || isMatchAll)
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
        val query = qDefs(relaxLevel)._1(w, w.length)
        val leastCount = qDefs(relaxLevel)._2
        val me = context.self

        search.setQuery(filteredQuery(query, filter)).execute(new ActionListener[SearchResponse] {
          override def onResponse(response: SearchResponse): Unit = {
            if (response.getHits.totalHits() >= leastCount || relaxLevel >= qDefs.length - 1)
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

      val parsedResult = parse(result.toString)
      /*.transformField {
        case JField("aggregations", obj: JValue) => JField("aggregations", obj.removeField(_._1=="areasyns").removeField(_._1=="products"))
      }.removeField(_._1=="_shards")*/


      val endTime = System.currentTimeMillis
      val timeTaken = endTime - startTime
      info("[" + result.getTookInMillis + "/" + timeTaken + (if(result.isTimedOut) " timeout" else "") + "] [q"+relaxLevel+"] [" + result.getHits.hits.length + "/" + result.getHits.getTotalHits + (if(result.isTerminatedEarly) " termearly ("+Math.min(maxdocspershard, int("max-docs-per-shard"))+")" else "") + "] [" + clip.toString + "]->[" + httpReq.uri + "]->[" + cats + "]")
      context.parent ! SearchResult(slug, result.getHits.hits.length, timeTaken, relaxLevel, parsedResult)
  }

  def urlize(k: String) =
    URLEncoder.encode(k.replaceAll(pat, " ").trim.replaceAll( """\s+""", "-").toLowerCase, "UTF-8")

}

