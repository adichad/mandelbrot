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
import org.elasticsearch.index.query.{BaseQueryBuilder, BoolFilterBuilder}
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.aggregations.bucket.nested.Nested
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.sort.{FieldSortBuilder, ScoreSortBuilder, SortBuilders, SortOrder}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConversions._


/**
 * Created by adichad on 08/01/15.
 */


object SearchRequestHandler extends Logging {

  val pat = """(?U)[^\p{alnum}]+"""
  private def addSort(search: SearchRequestBuilder, sort: String, lat: Double = 0d, lon: Double = 0d, areaSlugs: String = ""): Unit = {
    val parts = for (x <- sort.split(",")) yield x.trim
    parts.foreach {
      _ match {
        case "_score" => search.addSort(new ScoreSortBuilder().order(SortOrder.DESC))
        case "_distance" => search.addSort(
          SortBuilders.scriptSort("geobucket", "number").lang("native")
            .param("lat", lat).param("lon", lon).param("areaSlugs", areaSlugs).order(SortOrder.ASC))
        case x => {
          val pair = x.split( """\.""", 2)
          if (pair.size == 2)
            search.addSort(new FieldSortBuilder(pair(0)).order(SortOrder.valueOf(pair(1))))
          else if (pair.size == 1)
            search.addSort(new FieldSortBuilder(pair(0)).order(SortOrder.DESC))
        }
      }
    }
  }


  private[SearchRequestHandler] def nestIfNeeded(fieldName: String, q: BaseQueryBuilder): BaseQueryBuilder = {
    /*val parts = fieldName.split("""\.""")
    if (parts.length > 1)
      nestedQuery(parts(0), q).scoreMode("max")
    else
    */
    q
  }


  private[SearchRequestHandler] def shingleSpan(field: String, boost: Float, w: Array[String], fuzzyprefix: Int, fuzzysim: Float, maxShingle: Int, minShingle: Int = 1, sloppy: Boolean = true) = {
    val fieldQuery1 = boolQuery.minimumShouldMatch("67%")
    val terms = w
      .map(fuzzyQuery(field, _).prefixLength(fuzzyprefix).fuzziness(Fuzziness.TWO))
      .map(spanMultiTermQueryBuilder)

    (minShingle to Math.min(terms.length, maxShingle)).foreach { len =>
      terms.sliding(len).foreach { shingle =>
        val nearQuery = spanNearQuery.slop(if(sloppy)math.max(0,math.min(2,len - 3)) else 0).inOrder(!sloppy).boost(boost * len)
        shingle.foreach(nearQuery.clause)
        fieldQuery1.should(nearQuery)
      }
    }

    val fieldQuery2 = boolQuery
    val termsExact = w.map(spanTermQuery(field, _))
    (minShingle to Math.min(terms.length, maxShingle)).foreach { len =>
      var i = 100000
      termsExact.sliding(len).foreach { shingle =>
        val nearQuery = spanNearQuery.slop(if(sloppy)math.max(0,math.min(2,len - 3)) else 0).inOrder(!sloppy).boost(boost * 2 * len * len * math.max(1, i))
        shingle.foreach(nearQuery.clause)
        fieldQuery2.should(nearQuery)
        i /= 10
      }
    }
    nestIfNeeded(field, disMaxQuery.add(fieldQuery1).add(fieldQuery2))
  }

  private def strongMatch(fields: Map[String, Float],
                          condFields: Map[String, Map[String, Map[String, Float]]],
                          w: Array[String], kw: String, fuzzyprefix: Int, fuzzysim: Float, esClient: Client, index: String) = {

    val allQuery = boolQuery.minimumNumberShouldMatch(math.ceil(w.length.toFloat * 4f / 5f).toInt).boost(655360f)
    var i = 1000000
    w.foreach {
      word => {
        val posBoost = math.max(1, i)
        val wordQuery = boolQuery
        fields.foreach {
          field =>
            //wordQuery.should(nestIfNeeded(field._1, fuzzyQuery(field._1, word).prefixLength(fuzzyprefix).fuzziness(Fuzziness.ONE)))
            wordQuery.should(nestIfNeeded(field._1, termQuery(field._1, word).boost(262144f * field._2 * posBoost)))
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
                    answerQuery.should(termQuery(subField._1, word).boost(2f*subField._2 * posBoost))
                }
                perQuestionQuery.must(answerQuery)
                wordQuery.should(nestIfNeeded(cond._1, perQuestionQuery))
              }
            }
          }
        }
        allQuery.should(wordQuery)
        i /= 10
      }
    }
    val exactQuery = disMaxQuery
    val k = w.mkString(" ")
    val exactBoostFactor = 262144f * w.length * w.length * (searchFields.size + condFields.values.size + 1)*100
    fullFields.foreach {
      field: (String, Float) => {
        exactQuery.add(nestIfNeeded(field._1, termQuery(field._1, k).boost(field._2 * exactBoostFactor)))
      }
    }
    allQuery.should(exactQuery)
    allQuery.must(termQuery("CustomerType", "350"))
  }

  private def strongMatchNonPaid(fields: Map[String, Float],
                          condFields: Map[String, Map[String, Map[String, Float]]],
                          w: Array[String], kw: String, fuzzyprefix: Int, fuzzysim: Float, esClient: Client, index: String) = {

    val allQuery = boolQuery.minimumNumberShouldMatch(math.ceil(w.length.toFloat * 3f / 4f).toInt).boost(32768f)
    var i = 10000
    w.foreach {
      word => {
        val posBoost = math.max(1, i)
        val wordQuery = boolQuery
        fields.foreach {
          field =>
            //wordQuery.should(nestIfNeeded(field._1, fuzzyQuery(field._1, word).prefixLength(fuzzyprefix).fuzziness(Fuzziness.ONE)))
            wordQuery.should(nestIfNeeded(field._1, termQuery(field._1, word).boost(131072f * field._2 * posBoost)))
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
                    answerQuery.should(termQuery(subField._1, word).boost(2f*subField._2 * posBoost))
                }
                perQuestionQuery.must(answerQuery)
                wordQuery.should(nestIfNeeded(cond._1, perQuestionQuery))
              }
            }
          }
        }
        allQuery.should(wordQuery)
        i /= 10
      }
    }
    val k = w.mkString(" ")
    val exactBoostFactor = 262144f * w.length * w.length * (searchFields.size + condFields.values.size + 1)*100
    val exactQuery = disMaxQuery
    fullFields.foreach {
      field: (String, Float) => {
        exactQuery.add(nestIfNeeded(field._1, termQuery(field._1, k).boost(field._2 * exactBoostFactor)))
      }
    }

    allQuery.should(exactQuery)
  }

  private def matchAnalyzed(esClient: Client, index: String, field: String, text: String, keywords: Array[String]): Boolean = {
    analyze(esClient, index, field, text).deep == keywords.deep
  }

  private def weakMatchAnalyzed(esClient: Client, index: String, field: String, text: String, keywords: Array[String]): Boolean = {
    val textWords = analyze(esClient, index, field, text)
    if(keywords.length > textWords.length) false else textWords.zip(keywords).forall(x=>x._1==x._2)
  }

  private val catFilterFields = Set("Product.l3categoryexact", "Product.categorykeywordsexact")

  private case class CategoryFilter(query: BaseQueryBuilder, cats: Seq[String])

  private def categoryFilter(query: BaseQueryBuilder, mw: Array[String], cityFilter: BoolFilterBuilder, aggBuckets: Int, esClient: Client, index: String, esType: String, maxdocspershard: Int): CategoryFilter = {
    val cquery = disMaxQuery
    var hasClauses = false

    catFilterFields.foreach {
      field: (String) => {
        (1 to mw.length).foreach { len =>
          mw.sliding(len).foreach { shingle =>
            val ck = shingle.mkString(" ")
            if(ck.trim != "") {
              cquery.add(nestIfNeeded(field, termQuery(field, ck).boost(len * 1024)))
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
        //catFilter.should(queryFilter(shingleSpan("LocationName", 1f, mw, 1, 0.85f, mw.length, mw.length)).cache(false))
        catFilter.should(queryFilter(termQuery("LocationNameExact", mw.mkString(" "))).cache(false))
        catFilter.should(queryFilter(nestIfNeeded("Product.l3categoryexact", termQuery("Product.l3categoryexact", mw.mkString(" ")))).cache(true))
        catFilter.should(queryFilter(nestIfNeeded("Product.categorykeywordsexact", termQuery("Product.categorykeywordsexact", mw.mkString(" ")))).cache(true))
        catFilter.should(termsFilter("LocationName", mw:_*))
        catFilter.should(termsFilter("CompanyAliases", mw:_*))

        Seq("LocationNameExact", "CompanyAliasesExact").foreach {
          field: (String) => {
            (1 to mw.length).foreach { len =>
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
        CategoryFilter(filteredQuery(query, catFilter), cats)
      }
      else
        CategoryFilter(query, Seq[String]())
    }
    else CategoryFilter(query, Seq[String]())
  }

  private def analyze(esClient: Client, index: String, field: String, text: String): Array[String] =
    new AnalyzeRequestBuilder(esClient.admin.indices, index, text).setField(field).get().getTokens.map(_.getTerm).toArray


  private val searchFields = Map("LocationName" -> 512f, "CompanyAliases" -> 512f,
    "Product.l3category" -> 2048f, "LocationType"->1024f, "BusinessType"->1024f, "Product.name" -> 256f, "Product.brand" -> 256f,
    "Product.categorykeywords" -> 2048f, "Product.stringattribute.answer" -> 16f, "Area"->8f, "AreaSynonyms"->8f, "City"->1f, "CitySynonyms"->1f)

  private val condFields = Map(
    "Product.stringattribute.question" -> Map(
      "brands" -> Map("Product.stringattribute.answer" -> 1024f),
      "menu" -> Map("Product.stringattribute.answer" -> 512f),
      "destinations" -> Map("Product.stringattribute.answer" -> 512f),
      "product" -> Map("Product.stringattribute.answer" -> 256f),
      "tests" -> Map("Product.stringattribute.answer" -> 256f),
      "courses" -> Map("Product.stringattribute.answer" -> 256f)
    )
  )

  private val exactFields = Map("Product.categorykeywords" -> 1048576f/*, "CompanyAliases" -> 1048576f*/)

  private val exactFirstFields = Map("Product.l3category" -> 1048576f, "LocationName" -> 1048576f)

  private val fullFields = Map(
    "Product.l3categoryexact"->1048576f, "Product.categorykeywordsexact"->1048576f,
    "LocationNameExact"->1048577f, "CompanyAliasesExact"->1048577f,
    "Product.stringattribute.answerexact"->524288f)

  private val emptyStringArray = new Array[String](0)

}

class SearchRequestHandler(val config: Config, serverContext: SearchContext) extends Actor with Configurable with Logging {
  import SearchRequestHandler._
  private val esClient: Client = serverContext.esClient
  private var w = emptyStringArray

  private case class WrappedRequest(search: Option[SearchRequestBuilder], cats: Seq[String])

  private def buildSearch(searchParams: SearchParams): WrappedRequest = {
    import searchParams.filters._
    import searchParams.geo._
    import searchParams.idx._
    import searchParams.limits._
    import searchParams.page._
    import searchParams.text._
    import searchParams.view._


    var query: BaseQueryBuilder = null

    if (kw != null && kw.trim != "") {
      w = analyze(esClient, index, "CompanyName", kw)

      if (w.length > 0) {
        val kwquery = disMaxQuery

        searchFields.foreach {
          field: (String, Float) => {
            kwquery.add(shingleSpan(field._1, field._2, w, fuzzyprefix, fuzzysim, 4))
          }
        }

        condFields.foreach {
          field: (String, Map[String, Map[String, Float]]) => {
            val conditionalQuery = disMaxQuery
            field._2.foreach {
              v: (String, Map[String, Float]) => {
                val perQuestionQuery = boolQuery
                perQuestionQuery.must(nestIfNeeded(field._1, termQuery(field._1, v._1)))
                val answerQuery = disMaxQuery
                v._2.foreach {
                  subField: (String, Float) =>
                    answerQuery.add(shingleSpan(subField._1, subField._2, w, fuzzyprefix, fuzzysim, 4))
                }
                perQuestionQuery.must(answerQuery)
                conditionalQuery.add(perQuestionQuery)
              }
            }
            kwquery.add(conditionalQuery)
          }
        }

        exactFirstFields.foreach {
          field: (String, Float) => {
            val termsExact = w.map(spanTermQuery(field._1, _).boost(field._2))
            val nearQuery = spanNearQuery.slop(0).inOrder(true)
            termsExact.foreach(nearQuery.clause)
            kwquery.add(boolQuery.should(nestIfNeeded(field._1, spanFirstQuery(nearQuery, termsExact.length + 1))).boost(field._2 * 2 * w.length * w.length * (searchFields.size + condFields.values.size + 1)))
          }
        }
        exactFields.foreach {
          field: (String, Float) => {
            val termsExact = w.map(spanTermQuery(field._1, _).boost(field._2))
            val nearQuery = spanNearQuery.slop(0).inOrder(true)
            termsExact.foreach(nearQuery.clause)
            kwquery.add(boolQuery.should(nestIfNeeded(field._1, nearQuery)).boost(field._2 * 2 * w.length * w.length * (searchFields.size + condFields.values.size + 1)))
          }
        }

        fullFields.foreach {
          field: (String, Float) => {
            (math.max(1*w.length/2, 1) to w.length).foreach { len =>
              w.sliding(len).foreach { shingle =>
                val k = shingle.mkString(" ")
                kwquery.add(nestIfNeeded(field._1, termQuery(field._1, k).boost(field._2 * 2097152f * len * len * (searchFields.size + condFields.values.size + 1))))
              }
            }
          }
        }
        kwquery.add(strongMatch(searchFields, condFields, w, kw, fuzzyprefix, fuzzysim, esClient, index))

        kwquery.add(strongMatchNonPaid(searchFields, condFields, w, kw, fuzzyprefix, fuzzysim, esClient, index))
        query = kwquery
      } else if(category.trim == "" && id=="" && userid == 0 && locid == "") {
        context.parent ! EmptyResponse ("empty search criteria")
        return WrappedRequest(None,Seq())
      }
    } else if(category.trim == "" && id=="" && userid == 0 && locid == "") {
      context.parent ! EmptyResponse ("empty search criteria")
      return WrappedRequest(None,Seq())
    }

    // filters
    val cityFilter = boolFilter.cache(false)
    if (id != "")
      query = filteredQuery(query, idsFilter(esType).addIds(id.split( """,""").map(_.trim): _*))
    if(userid != 0)
      query = filteredQuery(query, termFilter("UserID", userid))
    if(locid != "") {
      query = filteredQuery(query, termsFilter("EDMSLocationID", locid.split(""",""").map(_.trim.toInt) :_*))
    }

    if (city != "") {

      val cityParams = city.split( """,""").map(_.trim.toLowerCase)
      cityFilter.should(termsFilter("City", cityParams: _*).cache(true))
      cityFilter.should(termsFilter("CitySynonyms", cityParams: _*).cache(false))
      cityFilter.should(termsFilter("CitySlug", cityParams: _*).cache(false))
      query = filteredQuery(query, cityFilter)
    }

    var cats = Seq[String]()

    if (category != "") {
      val cats = category.split("""#""")
      val b = boolFilter.cache(false)
      cats.foreach { c =>
        b.should(queryFilter(matchPhraseQuery("Product.l3category", c).slop(1)).cache(true))
        b.should(termFilter("Product.l3categoryslug", c).cache(false))
      }
      query = filteredQuery(query, nestedFilter("Product", b).cache(false))
    } else {
      val matchedCats = categoryFilter(query, w, cityFilter, aggbuckets, esClient, index, esType, Math.min(maxdocspershard, int("max-docs-per-shard")))
      query = matchedCats.query
      cats = matchedCats.cats

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
        val areaSpan = spanNearQuery.slop(1).inOrder(true)
        terms.foreach(areaSpan.clause)
        locFilter.should(queryFilter(areaSpan).cache(false))

        val synTerms = areaWords
          .map(fuzzyQuery("AreaSynonyms", _).prefixLength(1).fuzziness(Fuzziness.TWO))
          .map(spanMultiTermQueryBuilder)
        val synAreaSpan = spanNearQuery.slop(1).inOrder(true)
        synTerms.foreach(synAreaSpan.clause)
        locFilter.should(queryFilter(synAreaSpan).cache(false))
        analyzedAreas += areaWords.mkString("-")
      }

      areas.map(a => termFilter("City", a).cache(false)).foreach(locFilter.should)
      areas.map(a => termFilter("CitySynonyms", a).cache(false)).foreach(locFilter.should)
      areas.map(a => termFilter("AreaSlug", a).cache(false)).foreach(locFilter.should)

    }
    val areaSlugs = analyzedAreas.mkString("#")

    if (lat != 0.0d || lon != 0.0d)
      locFilter.should(
        geoDistanceRangeFilter("LatLong").cache(true)
          .point(lat, lon)
          .from((if (area == "") fromkm else 0.0d) + "km")
          .to((if (area == "") tokm else 10.0d) + "km")
          .optimizeBbox("indexed")
          .geoDistance(GeoDistance.SLOPPY_ARC))

    if (locFilter.hasClauses)
      query = filteredQuery(query, locFilter)
    if (pin != "")
      query = filteredQuery(query, termsFilter("PinCode", pin.split( """,""").map(_.trim): _*).cache(false))


    val search = esClient.prepareSearch(index.split(","): _*).setQueryCache(false)
      .setTypes(esType.split(","): _*)
      .setSearchType(SearchType.fromString(searchType))
      .setQuery(filteredQuery(query, boolFilter.mustNot(termFilter("DeleteFlag", 1l))))
      .setTrackScores(true)
      .addFields(select.split( ""","""): _*)
      .setFrom(offset).setSize(size)
      .setTimeout(TimeValue.timeValueMillis(Math.min(timeoutms, long("timeoutms"))))
      .setTerminateAfter(Math.min(maxdocspershard, int("max-docs-per-shard")))
      .setExplain(explain)
      .setFetchSource(source)

    addSort(search, sort, lat, lon, areaSlugs)

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
      search.addAggregation(
        terms("apl")
          .script("if([100, 275, 300, 350].grep(doc['custtype'].value)) return 1; else return 0;")
          .subAggregation(
            topHits("best").addSort(new ScoreSortBuilder().order(SortOrder.DESC)).setSize(size+offset)
          )
      )
*/
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
          .subAggregation(terms("kw").field("Product.categorykeywordsaggr").size(aggbuckets*3))
          .subAggregation(sum("sum_score").script("docscore").lang("native"))
        )
      )
      search.addAggregation(terms("areasyns").field("AreaAggr").size(aggbuckets)
        .subAggregation(terms("syns").field("AreaSynonymsAggr").size(aggbuckets))
      )
    }
    WrappedRequest(Some(search), cats)
  }


  case class WrappedResponse(searchParams: SearchParams, result: SearchResponse, cats: Seq[String])

  override def receive = {
    case searchParams: SearchParams =>

      val wreq = buildSearch(searchParams)
      val searchOpt = wreq.search
      val cats = wreq.cats
      searchOpt match {
      case None=>
      case Some(search) =>
        val me = context.self

        search.execute(new ActionListener[SearchResponse] {
          override def onResponse(response: SearchResponse): Unit = {
            me ! WrappedResponse(searchParams, response, cats)
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

      val cats = response.cats.toList

      val areaWords = analyze(esClient, index, "Area", area)

      var slug = ""
      var bestCatSlug = ""
      if (slugFlag) {
        val catBucks = result.getAggregations.get("products").asInstanceOf[Nested].getAggregations.get("catkw").asInstanceOf[Terms].getBuckets
        val matchedCat = catBucks
          .find(b => matchAnalyzed(esClient, index, "Product.l3category", b.getKey, w)
          || b.getAggregations.get("kw").asInstanceOf[Terms].getBuckets.exists(c => matchAnalyzed(esClient, index, "Product.categorykeywords", c.getKey, w)))
          .fold("/search/" + urlize(kw))(k => "/" + urlize(k.getKey))

        val bestCat = ""
        //result.getAggregations.get("products").asInstanceOf[Nested].getAggregations.get("catkw").asInstanceOf[Terms].getBuckets
        //  .headOption.fold("/search/" + urlize(kw))(k => "/" + urlize(k.getKey))

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

        bestCatSlug = ""
        //(if (city != "") "/" + urlize(city) else "") +
        //  bestCat +
        //  (if (category != "") "/cat/" + urlize(category) else "") +
        //  (if (area != "") matchedArea else "")
      }
      val endTime = System.currentTimeMillis
      val timeTaken = endTime - startTime
      info("[" + result.getTookInMillis + "/" + timeTaken + (if(result.isTimedOut) " timeout" else "") + "] [" + result.getHits.hits.length + "/" + result.getHits.getTotalHits + (if(result.isTerminatedEarly) " termearly ("+Math.min(maxdocspershard, int("max-docs-per-shard"))+")" else "") + "] [" + clip.toString + "]->[" + httpReq.uri + "]->[" + cats + "]")
      context.parent ! SearchResult(slug, bestCatSlug, result.getHits.hits.length, timeTaken, parse(result.toString))
  }

  def urlize(k: String) =
    URLEncoder.encode(k.replaceAll(pat, " ").trim.replaceAll( """\s+""", "-").toLowerCase, "UTF-8")

}

