package com.askme.mandelbrot.handler.aggregate.message

import com.askme.mandelbrot.handler.message.RestMessage
import org.json4s.JsonAST.JValue

/**
 * Created by adichad on 31/03/15.
 */
case class AggregateResult (`approx-total-count`: Long, `hit-count`: Int,
                            `server-time-ms`: Long, `terminated-early`: Boolean,
                            `timed-out`: Boolean, results: JValue) extends RestMessage
