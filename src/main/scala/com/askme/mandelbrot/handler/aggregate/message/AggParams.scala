package com.askme.mandelbrot.handler.aggregate.message

import com.askme.mandelbrot.handler.message.RestMessage

/**
 * Created by adichad on 11/04/15.
 */

case class AggParams(aggSpecs: Seq[AggSpec]) extends RestMessage
case class AggSpec(name: String, offset: Int, size: Int)