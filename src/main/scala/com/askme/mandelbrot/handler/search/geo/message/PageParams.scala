package com.askme.mandelbrot.handler.search.geo.message

import com.askme.mandelbrot.handler.message.RestMessage

/**
 * Created by adichad on 31/03/15.
 */
case class PageParams(size: Int, offset: Int) extends RestMessage
