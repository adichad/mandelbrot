package com.askme.mandelbrot.server

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.MandelbrotHandler
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.common.logging.ESLoggerFactory
import org.elasticsearch.common.logging.slf4j.Slf4jESLoggerFactory
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.node.NodeBuilder
import spray.can.Http

import scala.concurrent.duration.DurationInt

object RootServer extends Logging {

  class SearchContext private[RootServer](val config: Config) extends Configurable {
    ESLoggerFactory.setDefaultFactory(new Slf4jESLoggerFactory)

    /*
    val conf = (new com.hazelcast.config.Config)
      .setProperty("hazelcast.logging.type", string("hazel.logging.type"))


    private val netConf = conf.getNetworkConfig
    private val joinConf = netConf.getJoin
    joinConf.getMulticastConfig.setEnabled(boolean("hazel.multicast.enabled"))
    joinConf.getTcpIpConfig.setEnabled(boolean("hazel.tcpip.enabled"))
    joinConf.getTcpIpConfig.setMembers(list[String]("hazel.tcpip.members"))
    joinConf.getTcpIpConfig.setRequiredMember(string("hazel.tcpip.required.member"))
    netConf.setPort(int("hazel.port.number"))
    netConf.setPortAutoIncrement(boolean("hazel.port.autoincrement"))
    netConf.getInterfaces.setInterfaces(list[String]("hazel.interfaces"))
    netConf.getInterfaces.setEnabled(boolean("hazel.interface.enabled"))
    */
    //val hazel = Hazelcast.newHazelcastInstance(conf)

    private val esNode = NodeBuilder.nodeBuilder.clusterName(string("es.cluster.name")).local(false)
      .data(boolean("es.node.data")).settings(
      ImmutableSettings.settingsBuilder()
        .put("node.name", string("es.node.name"))
        .put("node.master", string("es.node.master"))
        .put("discovery.zen.ping.multicast.enabled", string("es.discovery.zen.ping.multicast.enabled"))
        .put("discovery.zen.ping.unicast.hosts", string("es.discovery.zen.ping.unicast.hosts"))
        .put("discovery.zen.minimum_master_nodes", string("es.discovery.zen.minimum_master_nodes"))
        .put("network.host", string("es.network.host"))
        .put("path.data", string("es.path.data"))
        .put("path.logs", string("es.path.logs"))
        .put("path.conf", string("es.path.conf"))
        .put("indices.cache.query.size", string("es.indices.cache.query.size"))
        .put("indices.cache.filter.size", string("es.indices.cache.filter.size"))
        .put("indices.memory.index_buffer_size",string("es.indices.memory.index_buffer_size"))
        .put("indices.memory.min_index_buffer_size",string("es.indices.memory.min_index_buffer_size"))
        .put("indices.memory.max_index_buffer_size",string("es.indices.memory.max_index_buffer_size"))
        .put("indices.fielddata.cache.size", string("es.indices.fielddata.cache.size"))
        .put("indices.store.throttle.max_bytes_per_sec",string("es.indices.store.throttle.max_bytes_per_sec"))
        .put("indices.store.throttle.type", string("es.indices.store.throttle.type"))
        .put("gateway.recover_after_nodes", string("es.gateway.recover_after_nodes"))
        .put("gateway.expected_nodes", string("es.gateway.expected_nodes"))
        .put("gateway.recover_after_time", string("es.gateway.recover_after_time"))
        .put("threadpool.search.type", string("es.threadpool.search.type"))
        .put("threadpool.search.size", string("es.threadpool.search.size"))
        .put("threadpool.search.queue_size", string("es.threadpool.search.queue_size"))
        .put("logger.index.search.slowlog.threshold.query.warn", string("es.logger.index.search.slowlog.threshold.query.warn"))
        .put("script.native.geobucket.type", "com.askme.mandelbrot.scripts.GeoBucket")
    ).node
    val esClient = esNode.client
    info("waiting for green status")
    info(esClient.admin().cluster.prepareHealth().setWaitForYellowStatus().get())
    info("optimizing")
    info(esClient.admin().indices().prepareOptimize().setMaxNumSegments(1).get())
    info("optimized")
    //val batchExecutionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(int("threads.batch")))
    //val userExecutionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(int("threads.user")))
/*
    // http://spark.apache.org/docs/latest/configuration.html
    private val sparkConf = (new SparkConf)
      .setMaster(string("spark.master"))
      .setAppName(string("spark.app.name"))
      .set("spark.executor.memory", string("spark.executor.memory"))
      .set("spark.shuffle.spill", string("spark.shuffle.spill"))
      .set("spark.logConf", string("spark.logConf"))
      .set("spark.local.dir", string("spark.local.dir"))
      //.set("spark.serializer", classOf[KryoSerializer].getName)

    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    val streamingContext = new StreamingContext(sparkContext, Seconds(int("spark.streaming.batch.duration")))
*/
    private[RootServer] def close() {

      //userExecutionContext.shutdown()
      //batchExecutionContext.shutdown()
      esClient.close()
      esNode.close()
      //sparkContext.stop()
      //hazel.shutdown()
    }
  }

}

class RootServer(val config: Config) extends Server with Logging {
  private implicit val system = ActorSystem(string("actorSystem.name"), conf("actorSystem"))
  private val serverContext = new RootServer.SearchContext(config)
  private val topActor = system.actorOf(Props(classOf[MandelbrotHandler], conf("handler"), serverContext), name = string("handler.name"))

  private implicit val timeout = Timeout(int("timeout").seconds)
  private val transport = IO(Http)


  override def bind {
    transport ! Http.Bind(topActor, interface = string("host"), port = int("port"))
    info("server bound: " + string("host") + ":" + int("port"))
  }

  override def close() {
    transport ? Http.Unbind
    serverContext.close()
    system.stop(topActor)
    system.shutdown()
    info("server shutdown complete: " + string("host") + ":" + int("port"))
  }

}
