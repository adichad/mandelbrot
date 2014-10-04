package com.askme.mandelbrot.admin

import java.net.InetSocketAddress
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelOption
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import com.askme.mandelbrot.server.Server
import io.netty.channel.socket.nio.NioServerSocketChannel

class NettyServer(val config: Config) extends Server with Logging {
  // keep for shutdown

  private val bossGroup = new NioEventLoopGroup(int("accept.threads"))
  private val workerGroup = new NioEventLoopGroup(int("io.threads"))

  // keep for binding
  private val bootstrap = (new ServerBootstrap).group(bossGroup, workerGroup)
    .channel(classOf[NioServerSocketChannel])
    .childHandler(obj[HttpChannelInitializer]("initializer"))
    .option[Integer](ChannelOption.SO_BACKLOG, int("accept.channel.SO_BACKLOG"))
    .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, boolean("accept.channel.SO_KEEPALIVE"))
    .option[java.lang.Boolean](ChannelOption.TCP_NODELAY, boolean("accept.channel.TCP_NODELAY"))
    .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, boolean("io.channel.SO_KEEPALIVE"))
    .childOption[java.lang.Boolean](ChannelOption.TCP_NODELAY, boolean("io.channel.TCP_NODELAY"))
    .localAddress(new InetSocketAddress(string("host"), int("port")))

  override def bind {
    bootstrap.bind.sync.channel.closeFuture.addListener(new ChannelFutureListener {
      override def operationComplete(cf: ChannelFuture) = {
        debug("bootstrap channel closed")
      }
    })
  }
  
  override def close {
    workerGroup shutdownGracefully ()
    bossGroup shutdownGracefully ()
    debug("server: " + bootstrap + " shutdown gracefully")
  }
}