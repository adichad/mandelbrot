package com.askme.mandelbrot.server

import java.io.Closeable

import com.askme.mandelbrot.Configurable


trait Server extends Closeable with Configurable {
  def bind: Unit
}
