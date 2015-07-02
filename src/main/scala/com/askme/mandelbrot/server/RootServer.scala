package com.askme.mandelbrot.server

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.MandelbrotHandler
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.Client
import org.elasticsearch.common.logging.ESLoggerFactory
import org.elasticsearch.common.logging.slf4j.Slf4jESLoggerFactory
import org.elasticsearch.node.NodeBuilder
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import spray.can.Http
import scala.collection.JavaConversions._
import scala.collection.convert.decorateAsScala._

import scala.concurrent.duration.DurationInt

object RootServer extends Logging {
  private var defContext: SearchContext = null
  def defaultContext = defContext
  private def analyze(esClient: Client, index: String, field: String, text: String): Array[String] =
    new AnalyzeRequestBuilder(esClient.admin.indices, index, text).setField(field).get().getTokens.map(_.getTerm).toArray

  private val vCache = new java.util.concurrent.ConcurrentHashMap[(String, String, String, String, String), Set[String]].asScala

  def uniqueVals(index: String, esType: String, field: String, analysisField: String, sep: String, maxCount: Int): Set[String] = {
    vCache.getOrElseUpdate((index, esType, field, analysisField, sep),
      defaultContext.esClient.prepareSearch(index).setTypes(esType).setSearchType(SearchType.COUNT).setSize(0).setTrackScores(false)
        .setQuery(matchAllQuery).addAggregation(terms(field).size(maxCount)).execute().get()
        .getAggregations.get(field).asInstanceOf[Terms].getBuckets
        .map(b=>analyze(defaultContext.esClient, index, analysisField, b.getKey).mkString(sep)).toSet)
  }
  class SearchContext private[RootServer](val config: Config) extends Configurable {
    ESLoggerFactory.setDefaultFactory(new Slf4jESLoggerFactory)

    private val esNode = NodeBuilder.nodeBuilder.clusterName(string("es.cluster.name")).local(false)
      .data(boolean("es.node.data")).settings(settings("es")).node
    val esClient = esNode.client
    RootServer.defContext = this

    private[RootServer] def close() {
      esClient.close()
      esNode.close()
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
