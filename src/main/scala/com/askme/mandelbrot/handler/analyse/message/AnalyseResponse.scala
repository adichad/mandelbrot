package com.askme.mandelbrot.handler.analyse.message

import com.askme.mandelbrot.handler.message.RestMessage
import org.json4s.JsonAST.JValue

/**
 * Created by adichad on 05/06/15.
 */
case class AnalyseResponse(`analysis-groups`: JValue, `server-time-ms`: Long) extends RestMessage
