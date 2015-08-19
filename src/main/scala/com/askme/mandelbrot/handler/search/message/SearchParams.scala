package com.askme.mandelbrot.handler.search.message

import com.askme.mandelbrot.handler.RequestParams
import com.askme.mandelbrot.handler.message.{IndexParams, RestMessage}

/**
 * Created by adichad on 31/03/15.
 */
case class SearchParams(req: RequestParams, idx: IndexParams, text: TextParams, geo: GeoParams, filters: FilterParams, page: PageParams, view: ViewParams, limits: LimitParams,
                        startTime: Long) extends RestMessage

case class DealSearchParams(req: RequestParams, idx: IndexParams, text: TextParams, geo: GeoParams, filters: DealFilterParams, page: PageParams, view: ViewParams, limits: LimitParams,
                        startTime: Long) extends RestMessage
