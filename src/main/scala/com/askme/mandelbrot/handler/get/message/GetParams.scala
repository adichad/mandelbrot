package com.askme.mandelbrot.handler.get.message

import com.askme.mandelbrot.handler.RequestParams
import com.askme.mandelbrot.handler.message.IndexParams
import com.askme.mandelbrot.handler.search.message.{LimitParams, PageParams, GeoParams}
import spray.http.{RemoteAddress, HttpRequest}

/**
 * Created by adichad on 21/07/15.
 */
case class GetParams(esType: String, id: String, select: String, transform: Boolean, req: HttpRequest, clip: RemoteAddress, startTime: Long)