package com.askme.mandelbrot.handler.search.geo.message

import com.askme.mandelbrot.handler.RequestParams
import com.askme.mandelbrot.handler.message.{RestMessage, IndexParams}

/**
  * Created by adichad on 26/02/16.
  */
case class GeoSearchParams(req: RequestParams, idx: IndexParams, text: TextParams, geo: GeoParams,
                           filters: FilterParams, container: ContainerParams, related: RelatedParams,
                           page: PageParams, view: ViewParams, limits: LimitParams,
                           startTime: Long) extends RestMessage
