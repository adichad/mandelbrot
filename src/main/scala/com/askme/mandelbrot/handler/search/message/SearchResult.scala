package com.askme.mandelbrot.handler.search.message

import com.askme.mandelbrot.handler.message.RestMessage
import org.json4s.JsonAST.{JDouble, JValue}

/**
 * Created by adichad on 31/03/15.
 */
case class SearchResult(`hit-count`: Int, `server-time-ms`: Long, relaxLevel: Int, results: JValue, lat: Double, lon: Double) extends RestMessage
case class SuggestResult(`server-time-ms`: Long, results: JValue) extends RestMessage
case class GetResult(`server-time-ms`: Long, version: Long, index: String, results: JValue) extends RestMessage