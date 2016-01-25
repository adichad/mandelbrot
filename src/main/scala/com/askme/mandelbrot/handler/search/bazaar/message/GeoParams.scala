package com.askme.mandelbrot.handler.search.bazaar.message

import com.askme.mandelbrot.handler.message.RestMessage

/**
 * Created by adichad on 31/03/15.
 */
case class GeoParams(city: String, area: String, pin: String, lat: Double, lon: Double, fromkm: Double, tokm: Double) extends RestMessage
