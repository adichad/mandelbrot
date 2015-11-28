package com.askme.mandelbrot.handler.aggregate.message

import com.askme.mandelbrot.handler.RequestParams
import com.askme.mandelbrot.handler.message.{IndexParams, RestMessage}
import com.askme.mandelbrot.handler.search.message.LimitParams

/**
 * Created by adichad on 31/03/15.
 */
case class AggregateParams(
                            req: RequestParams,
                            idx: IndexParams,
                            filter: FilterParams,
                            agg: AggParams,
                            lim: LimitParams,
                            startTime: Long) extends RestMessage

case class FilterParams(city: String, area: String, category: String, question: String, answer: String)
