package com.askme.mandelbrot.handler.search.geo.message

import com.askme.mandelbrot.handler.message.RestMessage

/**
 * Created by adichad on 31/03/15.
 */
case class ViewParams(source: Boolean, explain: Boolean, select: String,
                      searchType: String, version: Int) extends RestMessage
