package com.askme.mandelbrot.handler.list.message

import com.askme.mandelbrot.handler.message.RestMessage
import org.json4s.JsonAST.JValue

/**
 * Created by adichad on 31/03/15.
 */
case class AggregateResult (`total-count`: Int, `hit-count`: Int, `server-time-ms`: Long, results: JValue) extends RestMessage
