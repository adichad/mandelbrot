package com.askme.mandelbrot.handler.search.message

import com.askme.mandelbrot.handler.message.RestMessage
import org.json4s.JsonAST.JValue

/**
 * Created by adichad on 31/03/15.
 */
case class SearchResult(slug: String, `top-category-slug`: String, `hit-count`: Int, `server-time-ms`: Long, results: JValue) extends RestMessage
