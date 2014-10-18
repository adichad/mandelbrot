package com.askme.mandelbrot.server

import java.sql.DriverManager

import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.StreamAdminHandler
import com.hazelcast.core.Hazelcast
import org.elasticsearch.common.logging.ESLoggerFactory
import org.elasticsearch.common.logging.slf4j.Slf4jESLoggerFactory
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.node.NodeBuilder

import scala.concurrent.duration.DurationInt

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import spray.can.Http

object RootServer {

  class SearchContext private[RootServer](val config: Config) extends Configurable {
    ESLoggerFactory.setDefaultFactory(new Slf4jESLoggerFactory)
    val esNode = NodeBuilder.nodeBuilder.clusterName(string("es.cluster.name")).local(false).data(true).settings(
      ImmutableSettings.settingsBuilder()
        .put("node.name", string("es.node.name"))
    ).node
    val esClient = esNode.client

    private[RootServer] def close() {
      esClient.close()
      esNode.close()
    }
  }

  class PipelineContext private[RootServer] (val config: Config) extends Configurable with Logging {
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val conn = DriverManager.getConnection("jdbc:sqlserver://10.0.5.9;database=FastSynchronization;user=developer;password=developer@123")
    /*
    val statement = conn.prepareStatement("select top 10 * from [FastSynchronization].[eDMS].[GetitFlatfile]")

    val results = statement.executeQuery()
    while(results.next()) {
      for(i <- 0 to results.getMetaData.getColumnCount)
        info(results.getMetaData.getColumnName(i+1))
    }
    results.close()
    statement.close()
    */
    private[RootServer] def close() {

      conn.close()
    }
  }

}

class RootServer(val config: Config) extends Server with Logging {
  private implicit lazy val system = ActorSystem(string("actorSystem.name"), conf("actorSystem"))

  private val pipesContext = new RootServer.PipelineContext(config)
  private val searchContext = new RootServer.SearchContext(config)

  private val topActor = system.actorOf(Props(classOf[StreamAdminHandler], conf("handler"), searchContext), string("handler.name"))
  private implicit val timeout = Timeout(int("timeout").seconds)
  private lazy val transport = IO(Http)


  override def bind {
    //transport ? Http.Bind(topActor, interface = string("host"), port = int("port"))
    info("server bound: " + string("host") + ":" + int("port"))
  }

  override def close() {
    //transport ? Http.Unbind
    system.stop(topActor)
    system.shutdown()
    searchContext.close()
    pipesContext.close()
    Thread.sleep(200)
    info("server shutdown complete: " + string("host") + ":" + int("port"))
  }

}
