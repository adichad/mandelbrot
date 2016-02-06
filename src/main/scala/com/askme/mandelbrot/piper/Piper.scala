package com.askme.mandelbrot.piper

import akka.actor.{ActorRef, Actor}
import com.askme.mandelbrot.Configurable
import org.json4s.JValue

/**
 * Created by adichad on 08/07/15.
 */
trait Piper extends Configurable {
  def pipe(json: JValue, completer: ActorRef): Unit
}
