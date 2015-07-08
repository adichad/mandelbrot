package com.askme.mandelbrot.handler.analyse

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.analyse.message.{AnalyseResponse, AnalyseParams}
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder
import org.elasticsearch.client.Client
import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats

import org.json4s.JsonDSL.WithDouble._

import scala.collection.JavaConversions._

/**
 * Created by adichad on 05/06/15.
 */
class AnalyseRequestHandler(val config: Config, serverContext: SearchContext) extends Actor with Configurable with Logging {
  private val esClient: Client = serverContext.esClient

  implicit class ListOfTuplePimp[K, V](l: List[(K, V)]) {
    def toMultiMap = l.groupBy(_._1).map(x=>(x._1, x._2.map(_._2)))
  }

  override def receive = {
    case analyseParams: AnalyseParams =>
      import analyseParams.idx._
      import analyseParams._
      import analyseParams.req._

      implicit val formats = DefaultFormats
      val input = keywords ++ parse(if(data.isEmpty) "[]" else data).extract[List[String]]

      val result = render(map2jvalue(analyzers.map { analyzer =>
        analyzer ->
          input.map { text =>
            new AnalyzeRequestBuilder(esClient.admin().indices, index, text).get.getTokens.map(_.getTerm).toArray.mkString(" ") -> text
          }.toMultiMap
      }.toMultiMap.map(x=>(x._1, x._2(0)))))

      val endTime = System.currentTimeMillis
      val timeTaken = endTime - startTime
      info("[" + timeTaken + "] [" + clip.toString + "]->[" + httpReq.uri + "]")
      context.parent ! AnalyseResponse(result, timeTaken)
  }

}

