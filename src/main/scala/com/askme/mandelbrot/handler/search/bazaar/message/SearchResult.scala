package com.askme.mandelbrot.handler.search.bazaar.message

import com.askme.mandelbrot.handler.message.RestMessage
import org.json4s.JsonAST.JValue

/**
 * Created by adichad on 31/03/15.
 */
case class SearchResult(`hit-count`: Int, `server-time-ms`: Long, relaxLevel: Int, query: JValue, results: JValue) extends RestMessage

