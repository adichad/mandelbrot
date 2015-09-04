package com.askme.mandelbrot.handler.search.message

import com.askme.mandelbrot.handler.message.RestMessage

/**
 * Created by adichad on 31/03/15.
 */
case class FilterParams(category: String, id: String, userid: Int, locid: String) extends RestMessage
case class DealFilterParams(id: String, applicableTo: String, screentype: String) extends RestMessage
