package com.askme.mandelbrot.handler

import spray.routing.Route

/**
 * Created by adichad on 31/03/15.
 */
trait Router {
  def apply(implicit service: MandelbrotHandler): Route
}
