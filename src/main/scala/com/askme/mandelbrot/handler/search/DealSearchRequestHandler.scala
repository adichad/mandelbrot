package com.askme.mandelbrot.handler.search

import java.util
import java.util.concurrent.TimeUnit

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.EmptyResponse
import com.askme.mandelbrot.handler.message.ErrorResponse
import com.askme.mandelbrot.handler.search.message.{DealSearchParams, SearchResult}
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.analyze.{AnalyzeAction, AnalyzeRequestBuilder}
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse, SearchType}
import org.elasticsearch.client.Client
import org.elasticsearch.common.ParseFieldMatcher
import org.elasticsearch.common.unit.{Fuzziness, TimeValue}
import org.elasticsearch.index.query._
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.script.Script
import org.elasticsearch.script.ScriptService.ScriptType
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.sort._
import org.elasticsearch.search.sort.SortBuilders._
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConversions._

/**
 * Created by nishantkumar on 7/30/15.
 */

object DealSearchRequestHandler extends Logging {

  val pat = """(?U)[^\p{alnum}]+"""

  private def getSort(sort: String, lat: Double, lon: Double, areaSlugs: String) = {
    val parts = for (x <- sort.split(",")) yield x.trim
    parts.map {
      case "_pay" => scriptSort(new Script("dealpaysort", ScriptType.INLINE, "native", null), "number").order(SortOrder.ASC)
      case "_distance" =>
        val geoParams = new util.HashMap[String, AnyRef]
        geoParams.put("lat", double2Double(lat))
        geoParams.put("lon", double2Double(lon))
        geoParams.put("areaSlugs", areaSlugs)
        geoParams.put("areafield", "Locations.Area")
        geoParams.put("synfield", "Locations.AreaSynonyms")
        geoParams.put("skufield", "Locations.AreaSynonyms")
        geoParams.put("coordfield", "Locations.LatLong")
        scriptSort(new Script("geobucket", ScriptType.INLINE, "native", geoParams), "number").order(SortOrder.ASC)
      case "_featured" => new FieldSortBuilder("IsFeatured").order(SortOrder.DESC)
      case "_home" => new FieldSortBuilder("ShowOnHomePage").order(SortOrder.DESC)
      case "_score" => new ScoreSortBuilder().order(SortOrder.DESC)
      case "_channel" => scriptSort(new Script("dealchannelsort", ScriptType.INLINE, "native", null), "number").order(SortOrder.ASC)
      case "_base" => scriptSort(new Script("dealsort", ScriptType.INLINE, "native", null), "number").order(SortOrder.ASC)
    }
  }

  private def analyze(esClient: Client, index: String, field: String, text: String): Array[String] =
    new AnalyzeRequestBuilder(esClient.admin.indices, AnalyzeAction.INSTANCE, index, text).setField(field).get().getTokens.map(_.getTerm).toArray

  private implicit class DisMaxQueryPimp(val q: DisMaxQueryBuilder) {
    def addAll(queries: Iterable[QueryBuilder]) = {
      queries.foreach(q.add)
      q
    }
  }

  private def superBoost(len: Int) = math.pow(10, math.min(10,len+1)).toFloat
  private case class WrappedResponse(searchParams: DealSearchParams, result: SearchResponse, relaxLevel: Int)
  private case class ReSearch(searchParams: DealSearchParams, filter: BoolQueryBuilder, search: SearchRequestBuilder, relaxLevel: Int, response: SearchResponse)

  private def fuzzyOrTermQuery(field: String, word: String, exactBoost: Float, fuzzyPrefix: Int, fuzzy: Boolean = true) = {
    if(word.length > 8 && fuzzy)
      fuzzyQuery(field, word).prefixLength(fuzzyPrefix)
        .fuzziness(if(word.length > 12) Fuzziness.TWO else Fuzziness.ONE)
        .boost(if(word.length > 12) exactBoost/3f else exactBoost/2f)
    else
      termQuery(field, word).boost(exactBoost)

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

  private val emptyStringArray = new Array[String](0)
  private val searchFields2 = Map(
    "Title" -> 1000000000f,
    "Headline" -> 1000000000f,
    "Categories.Name" -> 10000000f,
    "Categories.Synonym" -> 1000f,
    "DealTags"->1000f,
    "Offers.Name" -> 1000f,
    "DescriptionShort" -> 100f,
    "Offers.DescriptionShort" -> 100f,
    "Locations.Area"->10f, "Locations.AreaSynonyms"->10f,
    "Locations.City"->1f, "Locations.CitySynonyms"->1f)

  private val fullFields2 = Map(
    "Title.TitleExact"->100000000000f,
    "Headline.HeadlineExact"->100000000000f,
    "Categories.Name.NameExact"->10000000000f,
    "Categories.Synonym.SynonymExact"->10000000f,
    "DealTags.DealTagsExact"->10000000f,
    "Offers.Name.NameExact"->1000f,
    "DescriptionShort.DescriptionShortExact"->1000f,
    "Offers.DescriptionShort.DescriptionShortExact" -> 1000f,
    "Locations.Area.AreaExact"->10f, "Locations.AreaSynonyms.AreaSynonymsExact"->10f,
    "Locations.City.CityExact"->1f, "Locations.CitySynonyms.CitySynonymsExact"->1f)

  private val exactToFieldMap = Map("Title.TitleExact" -> "Title",
    "Headline.HeadlineExact"-> "Headline",
    "Offers.Name.NameExact"-> "Offers.Name",
    "DescriptionShort.DescriptionShortExact" -> "DescriptionShort",
    "Offers.DescriptionShort.DescriptionShortExact" -> "Offers.DescriptionShort")

  private implicit class SearchPimp(val search: SearchRequestBuilder) {
    def addSorts(sorts: Iterable[SortBuilder]) = {
      sorts.foreach(search.addSort)
      search
    }
  }

  private[DealSearchRequestHandler] def shingleSpan(field: String, boost: Float, w: Array[String], fuzzyprefix: Int, maxShingle: Int, minShingle: Int = 1, sloppy: Boolean = true, fuzzy: Boolean = true) = {
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
    fieldQuery1
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
    fieldQuery
  }

  private def currQuery(tokenFields: Map[String, Float],
                        recomFields: Map[String, Float],
                        w: Array[String], fuzzy: Boolean = false, sloppy: Boolean = false, span: Boolean = false, tokenRelax: Int = 0) = {
    if(span)
      disMaxQuery.addAll(tokenFields.map(field => shingleSpan(field._1, field._2, w, 1, w.length, math.max(w.length-tokenRelax, 1), sloppy, fuzzy)))
    else
      disMaxQuery.addAll(recomFields.map(field =>
        if(exactToFieldMap isDefinedAt field._1)
          shingleSpan(exactToFieldMap(field._1), field._2, w, 1, w.length, math.max(w.length-tokenRelax, 1), sloppy, fuzzy)
        else
          shingleFull(field._1, field._2, w, 1, w.length, math.max(w.length-tokenRelax, 1), fuzzy)))
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

  private def queryBuilder(tokenFields: Map[String, Float], recomFields: Map[String, Float], fuzzy: Boolean = false, sloppy: Boolean = false, span: Boolean = false, minShingle: Int = 1, tokenRelax: Int = 0)
                          (w: Array[String], maxShingle: Int) = {
    shinglePartition(tokenFields, recomFields, w, maxShingle, minShingle, fuzzy, sloppy, span, tokenRelax)
  }

  private val qDefs: Seq[((Array[String], Int)=>QueryBuilder, Int)] = Seq(

    (queryBuilder(searchFields2, fullFields2, fuzzy = false, sloppy = false, span = false, 1, 0), 1), //1
    // full-shingle exact full matches

    //(queryBuilder(searchFields2, fullFields2, fuzzy = true, sloppy = false, span = false, 1, 0), 1), //3
    // full-shingle fuzzy full matches

    (queryBuilder(searchFields2, fullFields2, fuzzy = true, sloppy = true, span = true, 2, 1), 1) //7
    // relaxed-shingle exact span matches

  )
}

class DealSearchRequestHandler(val config: Config, serverContext: SearchContext) extends Actor with Configurable with Logging {

  import DealSearchRequestHandler._

  private val esClient: Client = serverContext.esClient
  private var kwids: Array[String] = emptyStringArray
  private var w = emptyStringArray
  private var lat: Double = 0d
  private var lon: Double = 0d
  private var areaSlugs = ""


  def buildSuggestQuery(kw: String, w: Array[String]) = {
    val query =
      if(kw.endsWith(" ")) {
        if (w.nonEmpty) {
          disMaxQuery.add(
            shinglePartitionSuggest(
              Map(
                "Title" -> 1e12f,
                "Headline"->1e11f,
                "Categories.Name" -> 1e11f,
                "Offers.Name" -> 1e11f,
                "DealTags" -> 1e11f),
              w, w.length, 1, fuzzy = false, sloppy = false, tokenRelax = 0
            )
          )/*.add(
            shinglePartitionSuggest(
              Map("name" -> 1e6f,
                "categories.name" -> 1e5f/*,
                "attributes_value" -> 1e5f*/),
              w, w.length, 1, fuzzy = true, sloppy = true, tokenRelax = 0
            )
          )*/.add(
            shinglePartitionSuggest(
              Map("Title.token_edge_ngram" -> 1e2f,
                "Headline.token_edge_ngram" -> 1e1f,
                "Categories.Name.token_edge_ngram" -> 1e1f,
                "Offers.Name.token_edge_ngram" -> 1e2f,
                "DealTags.token_edge_ngram" -> 1e2f),
              w, w.length, 1, fuzzy = false, sloppy = true, tokenRelax = 0
            )
          ).add(
            shinglePartitionSuggest(
              Map("Title.shingle_nospace_edge_ngram" -> 1e2f,
                "Headline.shingle_nospace_edge_ngram" -> 1e1f,
                "Categories.Name.shingle_nospace_edge_ngram" -> 1e1f,
                "Offers.Name.shingle_nospace_edge_ngram" -> 1e2f,
                "DealTags.shingle_nospace_edge_ngram" -> 1e2f),
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
              Map("Title" -> 1e12f,
                "Headline" -> 1e11f,
                "Categories.Name" -> 1e11f,
                "Offers.Name" -> 1e11f,
                "DealTags" -> 1e11f),
              w, w.length, 1, fuzzy = false, sloppy = false, tokenRelax = 0
            )
          )/*.add(
            shinglePartitionSuggest(
              Map("name" -> 1e6f,
                "categories.name" -> 1e5f/*,
                "Offers.Name" -> 1e5f*/),
              w, w.length, 1, fuzzy = true, sloppy = true, tokenRelax = 0
            )
          )*/.add(
            shinglePartitionSuggest(
              Map("Title.token_edge_ngram" -> 1e2f,
                "Headline.token_edge_ngram" -> 1e1f,
                "Categories.Name.token_edge_ngram" -> 1e1f,
                "Offers.Name.token_edge_ngram" -> 1e2f,
                "DealTags.token_edge_ngram" -> 1e2f),
              w, w.length, 1, fuzzy = false, sloppy = true, tokenRelax = 0
            )
          ).add(
            shinglePartitionSuggest(
              Map("Title.shingle_nospace_edge_ngram" -> 1e2f,
                "Headline.shingle_nospace_edge_ngram" -> 1e1f,
                "Categories.Name.shingle_nospace_edge_ngram" -> 1e1f,
                "Offers.Name.shingle_nospace_edge_ngram" -> 1e2f,
                "DealTags.shingle_nospace_edge_ngram" -> 1e2f),
              w, w.length, 1, fuzzy = false, sloppy = true, tokenRelax = 0
            )
          )

          val q2 = boolQuery
          if (front.nonEmpty) {
            q2.must(
              disMaxQuery.add(
                shinglePartitionSuggest(
                  Map("Title" -> 1e12f,
                    "Headline" -> 1e11f,
                    "Categories.Name" -> 1e11f,
                    "Offers.Name" -> 1e11f,
                    "DealTags" -> 1e11f),
                  front, front.length, 1, fuzzy = false, sloppy = false, tokenRelax = 0
                )
              )/*.add(
                shinglePartitionSuggest(
                  Map("name" -> 1e6f,
                    "categories.name" -> 1e5f/*,
                    "attributes_value" -> 1e5f*/),
                  front, front.length, 1, fuzzy = true, sloppy = true, tokenRelax = 0
                )
              )*/.add(
                shinglePartitionSuggest(
                  Map("Title.token_edge_ngram" -> 1e2f,
                    "Headline.token_edge_ngram" -> 1e1f,
                    "Categories.Name.token_edge_ngram" -> 1e1f,
                    "Offers.Name.token_edge_ngram" -> 1e2f,
                    "DealTags.token_edge_ngram" -> 1e2f),
                  front, front.length, 1, fuzzy = false, sloppy = true, tokenRelax = 0
                )
              ).add(
                shinglePartitionSuggest(
                  Map("Title.shingle_nospace_edge_ngram" -> 1e2f,
                    "Headline.shingle_nospace_edge_ngram" -> 1e1f,
                    "Categories.Name.shingle_nospace_edge_ngram" -> 1e1f,
                    "Offers.Name.shingle_nospace_edge_ngram" -> 1e2f,
                    "DealTags.shingle_nospace_edge_ngram" -> 1e2f),
                  front, front.length, 1, fuzzy = false, sloppy = true, tokenRelax = 0
                )
              )
            )
          }

          val q3 = disMaxQuery
            .add(fuzzyOrTermQuery("Title.token_edge_ngram", last_raw, 1e19f, 1, fuzzy = true))
          //.add(fuzzyOrTermQuery("Title.shingle_nospace_edge_ngram", last_raw, 1e17f, 1, fuzzy = true))

          q3.add(fuzzyOrTermQuery("Headline.token_edge_ngram", last_raw, 1e17f, 1, fuzzy = false))
          //.add(fuzzyOrTermQuery("Headline.shingle_nospace_edge_ngram", last_raw, 1e15f, 1, fuzzy = false))

          q3.add(fuzzyOrTermQuery("Categories.Name.token_edge_ngram", last_raw, 1e17f, 1, fuzzy = false))
          //.add(fuzzyOrTermQuery("Categories.Name.shingle_nospace_edge_ngram", last_raw, 1e15f, 1, fuzzy = false))

          q3.add(fuzzyOrTermQuery("Offers.Name.token_edge_ngram", last_raw, 1e17f, 1, fuzzy = false))
          //.add(fuzzyOrTermQuery("Offers.Name.shingle_nospace_edge_ngram", last_raw, 1e15f, 1, fuzzy = false))

          q3.add(fuzzyOrTermQuery("DealTags.token_edge_ngram", last_raw, 1e17f, 1, fuzzy = false))
          //.add(fuzzyOrTermQuery("DealTags.shingle_nospace_edge_ngram", last_raw, 1e15f, 1, fuzzy = false))

          q.add(if (q2.hasClauses) q2.must(q3) else q3)
        } else
          matchAllQuery
      }
    query
  }

  private def buildFilter(searchParams: DealSearchParams): BoolQueryBuilder = {
    import searchParams.filters._
    import searchParams.geo.{area, city}
    import searchParams.idx._

    val finalFilter = boolQuery

    if (!kwids.isEmpty) {
      finalFilter.must(idsQuery(esType).addIds(kwids: _*))
    }
    finalFilter.must(termQuery("Published", 1l))
    finalFilter.must(termQuery("Active", 1l))

    finalFilter.must(rangeQuery("EndDate").gt("now-1d")).must(rangeQuery("StartDate").lte("now"))
    finalFilter.must(rangeQuery("Offers.EndDate").gt("now-1d"))
    if (applicableTo != "" && pay_type==0) {
      finalFilter.must(termsQuery("ApplicableTo", applicableTo.split( ""","""):_*))
    }

    if(pay_merchant_id!="") {
      finalFilter.must(termQuery("Locations.PayMerchantID", pay_merchant_id))
    }
    if(edms_outlet_id!=0) {
      finalFilter.must(termQuery("Locations.eDMSLocationID", edms_outlet_id))
    }
    if(gll_outlet_id!=0) {
      finalFilter.must(termQuery("Locations.Oltp_Location_ID", gll_outlet_id))
    }

    if (category != "") {
      val catFilter = boolQuery
      val categories: Array[String] = category.split( """,""").map(analyze(esClient, index, "Categories.Name.NameExact", _).mkString(" ")).filter(!_.isEmpty)
      categories.map(fuzzyOrTermQuery("Categories.Name.NameExact", _, 1f, 1, fuzzy = true)).foreach(a => catFilter should a)
      finalFilter.must(catFilter)
    }


    val cities: Array[String] =
      if(city!="")
        city.split( """,""").map(analyze(esClient, index, "Locations.City.CityExact", _).mkString(" ")).filter(!_.isEmpty)
      else
        Array[String]()

    val areas: Array[String] =
      if(area!="")
        area.split(""",""").map(analyze(esClient, index, "Locations.Area.AreaExact", _).mkString(" ")).filter(!_.isEmpty)
      else
        Array[String]()



    val (myLat, myLon) = if (areas.nonEmpty) {
      areaSlugs = areas.mkString("#")
      if(searchParams.geo.lat == 0d && searchParams.geo.lon == 0d && areas.length==1 && cities.length == 1) {
        val latLongQuery = boolQuery().filter(
          boolQuery()
            .must(termQuery("types", "area"))
            .must(
              boolQuery()
                .should(termQuery("name.exact", areas.head))
                .should(termQuery("synonyms.exact", areas.head)))
            .must(
              nestedQuery("containers_dag",
                boolQuery()
                  .must(termQuery("containers_dag.types", "city"))
                  .must(
                    boolQuery()
                      .should(termQuery("containers_dag.name.exact", cities.head))
                      .should(termQuery("containers_dag.synonyms.exact", cities.head))
                  )
              )
            )
        )
        esClient.prepareSearch("geo").setTypes("geo").setFrom(0).setSize(1).setFetchSource("center",null)
          .setQuery(latLongQuery).execute().get(100, TimeUnit.MILLISECONDS).getHits.hits()
          .headOption
          .fold((searchParams.geo.lat, searchParams.geo.lon)) { hit =>
            val lon_lat = hit.getSource.get("center").asInstanceOf[util.ArrayList[Double]]
            (lon_lat(1), lon_lat(0))
          }
      }
      else
        (searchParams.geo.lat, searchParams.geo.lon)
    } else
      (searchParams.geo.lat, searchParams.geo.lon)
    lat = myLat
    lon = myLon

    if (lat != 0.0d || lon != 0.0d) {
      val locFilter = boolQuery.should(geoHashCellQuery("Locations.LatLong").point(lat, lon).precision("6km").neighbors(true))
      if (areas.nonEmpty) {
        areas.map(termQuery("Locations.Area.AreaExact", _)).foreach(a => locFilter should a)
        areas.map(termQuery("Locations.AreaSynonyms.AreaSynonymsExact", _)).foreach(a => locFilter should a)
        areas.map(termQuery("Locations.City.CityExact", _)).foreach(a => locFilter should a)
        areas.map(termQuery("Locations.CitySynonyms.CitySynonymsExact", _)).foreach(a => locFilter should a)
      }
      if (cities.nonEmpty) {
        cities.map(termQuery("VisiblePlaces.City.CityExact", _)).foreach(locFilter.should)
        cities.map(termQuery("VisiblePlaces.CitySynonyms.CitySynonymsExact", _)).foreach(locFilter.should)
        cities.map(termQuery("Locations.City.CityExact", _)).foreach(locFilter.should)
        cities.map(termQuery("Locations.CitySynonyms.CitySynonymsExact", _)).foreach(locFilter.should)
        locFilter.should(termQuery("DealDetail.VisibleToAllCities", true))
      }
      finalFilter.must(locFilter)
    } else {
      if (cities.nonEmpty) {
        val cityFilter = boolQuery
        cities.foreach { c =>
          cityFilter.should(termQuery("VisiblePlaces.City.CityExact", c))
          cityFilter.should(termQuery("VisiblePlaces.CitySynonyms.CitySynonymsExact", c))
          cityFilter.should(termQuery("Locations.City.CityExact", c))
          cityFilter.should(termQuery("Locations.CitySynonyms.CitySynonymsExact", c))
        }
        cityFilter.should(termQuery("DealDetail.VisibleToAllCities", true))

        if (cityFilter.hasClauses)
          finalFilter.must(cityFilter)
      }
    }

    if (featured) {
      finalFilter.must(termQuery("IsFeatured", 1l))
    }
    if (dealsource != "") {
      finalFilter.must(termQuery("DealSource.Name", dealsource))
    }
    finalFilter
  }

  private def buildSearch(searchParams: DealSearchParams): SearchRequestBuilder = {
    import searchParams.idx._
    import searchParams.limits._
    import searchParams.page._
    import searchParams.view._
    import searchParams.filters._
    import searchParams.text._


    val sort = if(pay_type==0) "_distance,_score,_featured,_home,_base,_channel" else "_distance,_score,_featured,_pay,_home,_base,_channel"

    val sorters = getSort(sort, lat, lon, areaSlugs)
    val search: SearchRequestBuilder = esClient.prepareSearch(index.split(","): _*)
      .setTypes(esType.split(","): _*)
      .setTrackScores(true)
      .setTimeout(TimeValue.timeValueMillis(Math.min(timeoutms, long("timeoutms"))))
      .setTerminateAfter(Math.min(maxdocspershard, int("max-docs-per-shard")))
      .setExplain(explain)
      .setSearchType(SearchType.fromString(searchType, ParseFieldMatcher.STRICT))
      .addSorts(sorters)
      .setFrom(offset).setSize(size)
    if (agg) {
      search.addAggregation(
        terms("categories").field("Categories.Name.NameAggr").size(if(suggest) 3 else aggbuckets).order(Terms.Order.count(false)))
      if(!suggest)
      search.addAggregation(
        terms("areas").field("Locations.Area.AreaAggr").size(aggbuckets).order(Terms.Order.count(false)))
      search.addAggregation(
        terms("tags").field("DealTags.DealTagsAggr").size(if(suggest) 3 else aggbuckets).order(Terms.Order.count(false)))
    }
    if(select == "") {
      search.setFetchSource(source)
    } else {
      search.setFetchSource(select.split(""","""), unselect.split(""","""))
    }
    search
  }

  override def receive = {
    case searchParams: DealSearchParams =>
      import searchParams.text._
      import searchParams.idx._
      import searchParams.filters._
      import searchParams.startTime
      import searchParams.req._
      import searchParams.geo.{area, city}
      try {
        kwids = id.split(",").map(_.trim.toUpperCase).filter(_.nonEmpty)
        val finalFilter = buildFilter(searchParams)

        w = if (kwids.length > 0) emptyStringArray else analyze(esClient, index, "Title", kw)
        if (w.length > 20) w = emptyStringArray
        w = w.take(8)
        if (w.isEmpty && kwids.isEmpty && category == "" && area == "" &&
            city == "" && applicableTo == "" && !featured && dealsource == "" && pay_merchant_id == "" && lat == 0d && lon == 0d) {
          context.parent ! EmptyResponse("empty search criteria")
        }
        val query = if(suggest) buildSuggestQuery(kw, w) else {
          if (w.length > 0) qDefs.head._1(w, w.length)
          else matchAllQuery()
        }
        val leastCount = qDefs.head._2
        val isMatchAll = query.isInstanceOf[MatchAllQueryBuilder]
        val search = buildSearch(searchParams)
        val me = context.self
        search.setQuery(boolQuery.must(query).filter(finalFilter))
        search.execute(new ActionListener[SearchResponse] {
          override def onResponse(response: SearchResponse): Unit = {
            if (response.getHits.totalHits() >= leastCount || isMatchAll)
              me ! WrappedResponse(searchParams, response, 0)
            else
              me ! ReSearch(searchParams, finalFilter, search, 1, response)
          }

          override def onFailure(e: Throwable): Unit = {
            val timeTaken = System.currentTimeMillis() - startTime
            error("[" + timeTaken + "] [q0] [na/na] [" + clip.toString + "]->[" + httpReq.uri + "]->[]", e)
            context.parent ! ErrorResponse(e.getMessage, e)
          }
        })
      } catch {
        case e: Throwable =>
          val timeTaken = System.currentTimeMillis() - startTime
          error("[" + timeTaken + "] [q0] [na/na] [" + clip.toString + "]->[" + httpReq.uri + "]->[]", e)
          context.parent ! ErrorResponse(e.getMessage, e)
      }

    case ReSearch(searchParams, filter, search, relaxLevel, response) =>
      import searchParams.startTime
      import searchParams.req._

      try {

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
              error("[" + timeTaken + "] [q" + relaxLevel + "] [na/na] [" + clip.toString + "]->[" + httpReq.uri + "]->[]", e)
              context.parent ! ErrorResponse(e.getMessage, e)
            }
          })
        }
      } catch {
        case e: Throwable =>
          val timeTaken = System.currentTimeMillis() - startTime
          error("[" + timeTaken + "] [q" + relaxLevel + "] [na/na] [" + clip.toString + "]->[" + httpReq.uri + "]->[]", e)
          context.parent ! ErrorResponse(e.getMessage, e)
      }

    case response: WrappedResponse =>
      import response.result
      import response.searchParams.limits._
      import response.searchParams.req._
      import response.searchParams.startTime
      import response.relaxLevel
      try {
        val endTime = System.currentTimeMillis
        val timeTaken = endTime - startTime
        val parsedResult = parse(result.toString)
        info("[" + result.getTookInMillis + "/" + timeTaken + (if (result.isTimedOut) " timeout" else "") + "] [q" + relaxLevel + "] [" + result.getHits.hits.length + "/" + result.getHits.getTotalHits + (if (result.isTerminatedEarly) " termearly (" + Math.min(maxdocspershard, int("max-docs-per-shard")) + ")" else "") + "] [" + clip.toString + "]->[" + httpReq.uri + "]")
        context.parent ! SearchResult(result.getHits.hits.length, timeTaken, relaxLevel, parsedResult, lat, lon)
      } catch {
        case e: Throwable =>
          val timeTaken = System.currentTimeMillis - startTime
          error("[" + timeTaken + "] [q" + relaxLevel + "] [na/na] [" + clip.toString + "]->[" + httpReq.uri + "]->[]", e)
          context.parent ! ErrorResponse(e.getMessage, e)
      }
  }
}
