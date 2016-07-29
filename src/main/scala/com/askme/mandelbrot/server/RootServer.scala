package com.askme.mandelbrot.server

import java.util

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.MandelbrotHandler
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import kafka.consumer.{KafkaStream, Whitelist, TopicFilter, ConsumerConfig}
import kafka.javaapi.consumer.SimpleConsumer
import kafka.javaapi.producer.Producer
import kafka.producer.ProducerConfig
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.elasticsearch.action.admin.indices.analyze.{AnalyzeAction, AnalyzeRequestBuilder}
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.Client
import org.elasticsearch.common.logging.ESLoggerFactory
import org.elasticsearch.common.logging.slf4j.Slf4jESLoggerFactory
import org.elasticsearch.node.NodeBuilder
import org.elasticsearch.node.MandelbrotNodeBuilder._
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.apache.kafka.clients.producer.KafkaProducer
import kafka.serializer.StringDecoder
import kafka.admin.AdminUtils

import spray.can.Http
import scala.collection.JavaConversions._
import scala.collection.convert.decorateAsScala._

import scala.concurrent.duration.DurationInt

object RootServer extends Logging {
  private var defContext: SearchContext = null
  def defaultContext = defContext
  private def analyze(esClient: Client, index: String, field: String, text: String): Array[String] =
    new AnalyzeRequestBuilder(esClient.admin.indices, AnalyzeAction.INSTANCE, index, text).setField(field).get().getTokens.map(_.getTerm).toArray

  private val vCache = new java.util.concurrent.ConcurrentHashMap[(String, String, String, String, String), Set[String]].asScala

  def uniqueVals(index: String, esType: String, field: String, analysisField: String, sep: String, maxCount: Int): Set[String] = {
    vCache.getOrElseUpdate((index, esType, field, analysisField, sep),
      defaultContext.esClient.prepareSearch(index).setTypes(esType)
        .setSize(0).setTerminateAfter(1000000).setTrackScores(false)
        .setQuery(matchAllQuery).addAggregation(terms(field).field(field).size(maxCount)).execute().get()
        .getAggregations.get(field).asInstanceOf[Terms].getBuckets
        .map(b=>analyze(defaultContext.esClient, index, analysisField, b.getKeyAsString).mkString(sep)).filter(!_.isEmpty).toSet)
  }
  class SearchContext private[RootServer](val parentPath: String) extends Configurable {
    ESLoggerFactory.setDefaultFactory(new Slf4jESLoggerFactory)

    private val esNode = NodeBuilder.nodeBuilder.clusterName(string("es.cluster.name")).local(false)
      .data(boolean("es.node.data")).settings(settings("es")).nodeCustom
/*
    private val zkClient = new ZkClient(
      string("kafka.zookeeper.connect"), int("kafka.zookeeper.session-timeout"),
      int("kafka.zookeeper.connect-timeout"), ZKStringSerializer
    )
    confs("kafka.topics").foreach { c =>
      if(!AdminUtils.topicExists(zkClient, c.getString("name"))) {
        AdminUtils.createTopic(
          zkClient, c.getString("name"),
          c.getInt("partitions"), c.getInt("replication"),
          props(c.getConfig("conf")))
        info("topic created: "+c.getString("name"))
      }
    }
*/
    val esClient = esNode.client
  /*
    val kafkaProducer = new Producer[String, String](new ProducerConfig(props("kafka.producer")))
    val consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props("kafka.consumer.conf")))
    val kafkaStreams: Map[String, util.List[KafkaStream[String, String]]] = list[String]("kafka.consumer.topics").toSet[String].map(t=>t -> consumerConnector.createMessageStreamsByFilter(Whitelist(t), 1, new StringDecoder(), new StringDecoder())).toMap
  */
    RootServer.defContext = this

    private[RootServer] def close() {
      esClient.close()
      //kafkaProducer.close
      //consumerConnector.shutdown()
      //zkClient.close()
      esNode.close()

    }
  }

}

class RootServer(val parentPath: String) extends Server with Logging {
  private implicit val system = ActorSystem(string("actorSystem.name"), conf("actorSystem"))
  private val serverContext = new RootServer.SearchContext(parentPath)
  private val topActor = system.actorOf(Props(classOf[MandelbrotHandler], (if (parentPath==null) "" else parentPath + ".") + "handler", serverContext), name = string("handler.name"))

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
