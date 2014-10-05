package com.askme.mandelbrot.handler

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequest
import org.elasticsearch.client.Client
import org.elasticsearch.common.io.stream.StreamOutput
import spray.http.MediaTypes.`text/html`
import spray.routing.HttpService
import spray.routing.Directive.pimpApply

class StreamAdminHandler(val config: Config, val esClient: Client) extends HttpService with Actor with Logging with Configurable {
  private val myRoute =
    path("") {
      get {
        respondWithMediaType(`text/html`) {
          complete {
            <html>
              <body>
                <h1>Say hello to <i>spray-routing</i> on <i>spray-can</i>!</h1>
                {esClient.settings.getAsMap.toString}
                {
                  esClient.admin.cluster.clusterStats(new ClusterStatsRequest).get.toString

                }
              </body>
            </html>

          }
        }
      }
    }
  
  def receive = runRoute(myRoute)
  def actorRefFactory = context
}
