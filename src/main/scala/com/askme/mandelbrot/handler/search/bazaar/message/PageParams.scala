package com.askme.mandelbrot.handler.search.bazaar.message

import com.askme.mandelbrot.handler.message.RestMessage

/**
 * Created by adichad on 31/03/15.
 */
case class PageParams(sort: String, size: Int, offset: Int, subscriptions_size: Int, subscriptions_offset: Int) extends RestMessage
