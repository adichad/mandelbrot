package com.askme.mandelbrot.server

import java.sql.DriverManager

import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.StreamAdminHandler
import com.hazelcast.config.NetworkConfig
import com.hazelcast.core.Hazelcast
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.common.logging.ESLoggerFactory
import org.elasticsearch.common.logging.slf4j.Slf4jESLoggerFactory
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.node.NodeBuilder

import org.apache.spark.deploy.worker._

import scala.concurrent.duration.DurationInt

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import spray.can.Http
import org.apache.spark.SparkContext._
import scala.collection.JavaConversions._

object RootServer {

  class SearchContext private[RootServer](val config: Config) extends Configurable {
    ESLoggerFactory.setDefaultFactory(new Slf4jESLoggerFactory)
    val esNode = NodeBuilder.nodeBuilder.clusterName(string("cluster.name")).local(false).data(true).settings(
      ImmutableSettings.settingsBuilder()
        .put("node.name", string("node.name"))
        .put("discovery.zen.ping.multicast.enabled", string("discovery.zen.ping.multicast.enabled"))
        .put("discovery.zen.ping.unicast.hosts", string("discovery.zen.ping.unicast.hosts"))
        .put("network.host", string("network.host"))
        .put("path.data", string("path.data"))
    ).node
    val esClient = esNode.client

    private[RootServer] def close() {
      esClient.close()
      esNode.close()
    }
  }

  class HazelContext private[RootServer](val config: Config) extends Configurable with Logging {
    val conf = (new com.hazelcast.config.Config)
      .setProperty("hazelcast.logging.type", string("logging.type"))


    val netConf = conf.getNetworkConfig
    val joinConf = netConf.getJoin
    joinConf.getMulticastConfig.setEnabled(boolean("multicast.enabled"))
    joinConf.getTcpIpConfig.setEnabled(boolean("tcpip.enabled"))
    joinConf.getTcpIpConfig.setMembers(list[String]("tcpip.members"))
    joinConf.getTcpIpConfig.setRequiredMember(string("tcpip.required.member"))
    netConf.setPort(int("port.number"))
    netConf.setPortAutoIncrement(boolean("port.autoincrement"))
    netConf.getInterfaces.setInterfaces(list[String]("interfaces"))
    netConf.getInterfaces.setEnabled(boolean("interface.enabled"))

    val hazel = Hazelcast.newHazelcastInstance(conf)


    private[RootServer] def close() {
      hazel.shutdown()
    }
  }

  class PipelineContext private[RootServer] (val config: Config) extends Configurable with Logging {
    //Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
    //val conn = DriverManager.getConnection("jdbc:sqlserver://10.0.5.9;database=FastSynchronization;user=developer;password=developer@123")
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

      //conn.close()
    }
  }

  class SparkApp private[RootServer] (val config: Config) extends Configurable with Logging {
    val sc = new SparkContext(new SparkConf()
      .setAppName("mandelbrot")
      .setMaster("spark://GIMAZSRCHDAP002:7077")
      .set("spark.logConf", "true")
      .set("spark.driver.host", "192.168.9.51")
    )


    val counts = sc.textFile("/Users/adichad/Downloads/adventures.of.sherlock.holms.txt").flatMap(line => line.split(" "))
      .map(word => (word, 1)).reduceByKey(_ + _)
    try {
      info("COUNTS: " + counts.saveAsTextFile("/Users/adichad/Downloads/adventures.of.sherlock.holms.wordcount.txt"))
    } catch {
      case e: Throwable =>
        error("local error received",e)
        throw e
    }

    //val conn = DriverManager.getConnection("jdbc:sqlserver://10.0.5.9;database=FastSynchronization;user=developer;password=developer@123")
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
      sc.stop()
    }
  }

}

class RootServer(val config: Config) extends Server with Logging {
  private implicit lazy val system = ActorSystem(string("actorSystem.name"), conf("actorSystem"))
  private val hazelContext = new RootServer.HazelContext(conf("hazel"))
  private val pipesContext = new RootServer.PipelineContext(config)
  private val searchContext = new RootServer.SearchContext(conf("es"))

  //private val sparkApp = new RootServer.SparkApp(config)

  private val topActor = system.actorOf(Props(classOf[StreamAdminHandler], conf("handler"), searchContext), string("handler.name"))
  private implicit val timeout = Timeout(int("timeout").seconds)
  private lazy val transport = IO(Http)


  override def bind {
    transport ? Http.Bind(topActor, interface = string("host"), port = int("port"))
    info("server bound: " + string("host") + ":" + int("port"))
  }

  override def close() {
    //transport ? Http.Unbind
    system.stop(topActor)
    system.shutdown()
    searchContext.close()
    pipesContext.close()
    hazelContext.close()
    Thread.sleep(200)
    info("server shutdown complete: " + string("host") + ":" + int("port"))
  }

}
