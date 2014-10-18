package com.askme.mandelbrot.handler

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequest
import spray.http.MediaTypes.`text/html`
import spray.routing.Directive.pimpApply
import spray.routing.HttpService

class StreamAdminHandler(val config: Config, serverContext: SearchContext) extends HttpService with Actor with Logging with Configurable {

  private val esClient = serverContext.esClient
  private val myRoute =
    path("") {
      get {
        respondWithMediaType(`text/html`) {
          complete {
            <html>
              <body>
                <h1>Say hello to <i>spray-routing</i> on <i>spray-can</i>!</h1>
                {esClient.settings.getAsMap.toString}
                {esClient.admin.cluster.clusterStats(new ClusterStatsRequest).get.toString}
              </body>
            </html>
          }
        }
      }
    }

  override def receive =
    runRoute(myRoute)

  override def postStop = {
    info("Kill Message received")
  }

  override def actorRefFactory = context
}
