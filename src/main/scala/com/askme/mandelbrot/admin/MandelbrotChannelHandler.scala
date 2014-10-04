package com.askme.mandelbrot.admin
import com.askme.mandelbrot.Configurable
import grizzled.slf4j.Logging
import io.netty.channel.SimpleChannelInboundHandler

abstract class MandelbrotChannelHandler[T] extends SimpleChannelInboundHandler[T] with Configurable with Logging {
  
}