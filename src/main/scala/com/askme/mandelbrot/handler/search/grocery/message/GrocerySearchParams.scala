package com.askme.mandelbrot.handler.search.grocery.message

import com.askme.mandelbrot.handler.RequestParams
import com.askme.mandelbrot.handler.message.{IndexParams, RestMessage}

/**
 * Created by adichad on 31/03/15.
 */
case class GrocerySearchParams(req: RequestParams, idx: IndexParams, text: TextParams,
                               filters: FilterParams, page: PageParams, view: ViewParams, limits: LimitParams,
                               startTime: Long) extends RestMessage
