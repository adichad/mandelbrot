package com.askme.mandelbrot.handler.list.message

import com.askme.mandelbrot.handler.RequestParams
import com.askme.mandelbrot.handler.message.{IndexParams, RestMessage}
import com.askme.mandelbrot.handler.search.message.{LimitParams, PageParams}

/**
 * Created by adichad on 31/03/15.
 */
case class AggregateParams(
                            req: RequestParams,
                            idx: IndexParams,
                            agg: AggregateFilterParams,
                            page: PageParams,
                            lim: LimitParams,
                            startTime: Long) extends RestMessage

case class AggregateFilterParams(city: String, area: String, category: String, agg: String)
