package com.askme.mandelbrot.handler

import akka.actor.Actor
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config

/**
 * Created by adichad on 08/01/15.
 */
class SearchRequestHandler(config: Config, serverContext: SearchContext) extends Actor {

  override def receive = {
    case searchParams: SearchParams =>
  }

}
