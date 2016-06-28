package com.askme.mandelbrot.handler.search.cantorish.message

import com.askme.mandelbrot.handler.message.RestMessage

/**
 * Created by adichad on 31/03/15.
 */
case class TextParams(kw: String, suggest: Boolean) extends RestMessage
