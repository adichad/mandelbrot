package com.askme.mandelbrot.handler

import akka.actor.{Kill, Actor}
import com.askme.mandelbrot.Configurable
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequest
import org.elasticsearch.common.logging.ESLoggerFactory
import org.elasticsearch.common.logging.slf4j.Slf4jESLoggerFactory
import org.elasticsearch.node.NodeBuilder
import spray.http.MediaTypes.`text/html`
import spray.routing.Directive.pimpApply
import spray.routing.HttpService

class StreamAdminHandler(val config: Config) extends HttpService with Actor with Logging with Configurable {
  ESLoggerFactory.setDefaultFactory(new Slf4jESLoggerFactory)
  private val esNode = NodeBuilder.nodeBuilder.clusterName(string("es.cluster.name")).local(false).data(true).node
  private val esClient = esNode.client
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
    esNode.close
  }

  override def actorRefFactory = context
}
