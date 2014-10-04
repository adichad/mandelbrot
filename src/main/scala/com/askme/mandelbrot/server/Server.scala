package com.askme.mandelbrot.server

import java.io.Closeable
import java.net.InetSocketAddress
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelHandler
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.http.HttpContentCompressor
import io.netty.handler.codec.http.HttpServerCodec
import com.askme.mandelbrot.Configurable
import io.netty.channel.socket.nio.NioServerSocketChannel
import com.askme.mandelbrot.admin.AdminHandler
import akka.actor.ActorSystem
import akka.actor.Props
import akka.util.Timeout
import akka.io.IO
import spray.can.Http
import akka.actor.Actor


trait Server extends Closeable with Configurable {
  def bind: Unit
}
