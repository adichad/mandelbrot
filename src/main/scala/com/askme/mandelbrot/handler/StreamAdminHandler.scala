package com.askme.mandelbrot.handler

import spray.http.MediaTypes.{`text/html` => `text/html`}
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.routing.Directive.pimpApply
import spray.routing.HttpService
import akka.actor.Actor
import com.typesafe.config.Config
import com.askme.mandelbrot.Configurable
import grizzled.slf4j.Logging

class StreamAdminHandler(val config: Config) extends HttpService with Actor with Logging with Configurable {
  private val myRoute =
    path("") {
      get {
        respondWithMediaType(`text/html`) {
          complete {
            <html>
              <body>
                <h1>Say hello to <i>spray-routing</i> on <i>spray-can</i>!</h1>
              </body>
            </html>
          }
        }
      }
    }
  
  def receive = runRoute(myRoute)
  def actorRefFactory = context
}
