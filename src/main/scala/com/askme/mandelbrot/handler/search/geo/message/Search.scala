package com.askme.mandelbrot.handler.search.geo.message

import org.elasticsearch.action.search.SearchRequestBuilder

/**
 * Created by adichad on 31/03/15.
 */
case class Search(search: SearchRequestBuilder, w: Array[String])
