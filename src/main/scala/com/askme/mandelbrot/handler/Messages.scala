package com.askme.mandelbrot.handler

import org.elasticsearch.action.search.SearchRequestBuilder
import org.json4s.JsonAST.JValue
import spray.http.{HttpRequest, RemoteAddress}

/**
 * Created by adichad on 14/01/15.
 */

trait RestMessage
case class SearchParams(req: RequestParams, idx: IndexParams, text: TextParams, geo: GeoParams, filters: FilterParams, page: PageParams, view: ViewParams, limits: LimitParams,
                        startTime: Long) extends RestMessage
case class IndexParams(index: String, esType: String) extends RestMessage
case class GeoParams(city: String, area: String, pin: String, lat: Double, lon: Double, fromkm: Double, tokm: Double) extends RestMessage
case class PageParams(size: Int, offset: Int) extends RestMessage
case class FilterParams(category: String, id: String) extends RestMessage
case class ViewParams(source: Boolean, agg: Boolean, aggbuckets: Int, explain: Boolean, sort: String, select: String, searchType: String, slugFlag: Boolean) extends RestMessage
case class LimitParams(maxdocspershard: Int, timeoutms: Long) extends RestMessage
case class TextParams(kw: String, fuzzyprefix: Int, fuzzysim: Float) extends RestMessage
case class RequestParams(httpReq: HttpRequest, clip: RemoteAddress, trueClient: String) extends RestMessage
case class SearchResult(slug: String, `hit-count`: Int, `server-time-ms`: Long, results: JValue) extends RestMessage
case class EmptyResponse(reason: String) extends RestMessage

case class IndexingParams(req: RequestParams, idx: IndexParams, data: RawData, startTime: Long) extends RestMessage
case class RawData(data: String) extends RestMessage

case class IndexResult(success: Boolean)

case class Search(search: SearchRequestBuilder, w: Array[String])



