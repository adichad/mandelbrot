package com.askme.mandelbrot.handler.search.grocery.message

import com.askme.mandelbrot.handler.message.RestMessage

/**
 * Created by adichad on 31/03/15.
 */
case class ViewParams(source: Boolean, agg: Boolean, aggbuckets: Int, extended_agg: Boolean, explain: Boolean, select: String,
                      searchType: String, version: Int) extends RestMessage
