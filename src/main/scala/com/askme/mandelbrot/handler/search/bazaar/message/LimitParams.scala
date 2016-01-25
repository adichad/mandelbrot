package com.askme.mandelbrot.handler.search.bazaar.message

import com.askme.mandelbrot.handler.message.RestMessage

/**
 * Created by adichad on 31/03/15.
 */
case class LimitParams(maxdocspershard: Int, timeoutms: Long) extends RestMessage
