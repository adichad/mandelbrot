package com.askme.mandelbrot.handler.suggest.message

import com.askme.mandelbrot.handler.RequestParams
import com.askme.mandelbrot.handler.message.IndexParams
import com.askme.mandelbrot.handler.search.message.{LimitParams, PageParams, GeoParams}

/**
 * Created by adichad on 21/07/15.
 */
case class SuggestParams(req: RequestParams, idx: IndexParams, target: TargetingParams, geo: GeoParams, page: PageParams, view: SuggestViewParams, limits: LimitParams, startTime: Long)

case class TargetingParams(kw: String, tag: String, id: String)

case class SuggestViewParams(explain: Boolean, select: String, unselect: String, searchType: String, version: Int)