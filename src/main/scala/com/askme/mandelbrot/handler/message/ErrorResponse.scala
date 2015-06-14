package com.askme.mandelbrot.handler.message

/**
 * Created by adichad on 15/06/15.
 */
case class ErrorResponse(val message: String, val e: Throwable) extends RestMessage
