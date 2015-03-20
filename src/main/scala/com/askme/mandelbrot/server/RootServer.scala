package com.askme.mandelbrot.server

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.MandelbrotHandler
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.apache.kafka.clients.producer.KafkaProducer
import org.elasticsearch.common.logging.ESLoggerFactory
import org.elasticsearch.common.logging.slf4j.Slf4jESLoggerFactory
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
      .data(boolean("es.node.data")).settings(settings("es")).node
    val esClient = esNode.client

    val kafkaProducer = new KafkaProducer[Array[Byte], Array[Byte]](props("kafka.producer"))

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
      kafkaProducer.close()
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
