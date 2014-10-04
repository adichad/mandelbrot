package com.askme.mandelbrot.admin

import com.typesafe.config.Config

import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.http.DefaultFullHttpResponse
import io.netty.handler.codec.http.HttpHeaders.Names
import io.netty.handler.codec.http.HttpHeaders.Values
import io.netty.handler.codec.http.HttpObject
import io.netty.handler.codec.http.HttpRequest
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.handler.codec.http.HttpVersion


class AdminHandler(val config: Config) extends MandelbrotChannelHandler[HttpObject] {
  
  override def channelRead0(ctx: ChannelHandlerContext, obj: HttpObject) {
    if (obj.isInstanceOf[HttpRequest]) {
      val req = obj.asInstanceOf[HttpRequest]
      val isMultipart = req.headers.contains(Names.CONTENT_TYPE, Values.MULTIPART_FORM_DATA, true)
      
      val response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
      response.content.setBytes(req.toString.getBytes.length, req.toString.getBytes)
      //response.content.alloc.buffer(bytes.length).setBytes(0, bytes)
      response.headers.set(Names.CONTENT_TYPE, "text/plain; charset=UTF-8")
      
      val channel = ctx writeAndFlush response
      debug("http request: " + obj.toString)
    } else {
      ctx.channel.close
      debug("not http request: " + obj.toString)
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    debug(cause.getMessage, cause)
    ctx.writeAndFlush(cause.getMessage).channel.close
  }
}
