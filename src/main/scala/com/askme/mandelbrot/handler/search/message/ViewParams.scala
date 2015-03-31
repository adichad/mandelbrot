package com.askme.mandelbrot.handler.search.message

import com.askme.mandelbrot.handler.message.RestMessage

/**
 * Created by adichad on 31/03/15.
 */
case class ViewParams(source: Boolean, agg: Boolean, aggbuckets: Int, explain: Boolean, sort: String, select: String, searchType: String, slugFlag: Boolean) extends RestMessage
