package com.askme.mandelbrot.admin

import com.askme.mandelbrot.Configurable
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.http.HttpContentCompressor
import io.netty.handler.codec.http.HttpServerCodec
import io.netty.handler.codec.http.HttpObject

class HttpChannelInitializer(val config: Config) extends ChannelInitializer[SocketChannel] with Logging with Configurable {

  override def initChannel(ch: SocketChannel) {
    ch.pipeline.addLast(
      new HttpServerCodec, new HttpContentCompressor)
    list[MandelbrotChannelHandler[HttpObject]]("handlers").foreach(ch.pipeline addLast _)
    debug("channel initialized")
  }

}