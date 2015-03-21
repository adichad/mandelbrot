package com.askme.mandelbrot.handler

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.BulkResponse
import org.json4s._
import org.json4s.jackson.JsonMethods._
import spray.json._
import spray.json.DefaultJsonProtocol._
import spray.json.{CompactPrinter, JsArray, JsValue, JsonParser}


/**
 * Created by adichad on 19/03/15.
 */
class IndexRequestHandler(val config: Config, serverContext: SearchContext) extends Actor with Logging with Configurable {

  //private val producer = serverContext.kafkaProducer

  private val esClient = serverContext.esClient


  override def receive = {
    case indexParams: IndexingParams =>
      val completer = context.parent
      try {
        val json = JsonParser(indexParams.data.data).asInstanceOf[JsArray]
        val bulkRequest = esClient.prepareBulk
        import spray.json.lenses.JsonLenses._
        for (doc: JsValue <- json.elements) {
          bulkRequest.add(
            esClient.prepareIndex(indexParams.idx.index, indexParams.idx.esType, doc.extract[String]('PlaceID))
              .setSource(CompactPrinter(doc))
          )
          /*producer.send(
          new ProducerRecord(
            indexParams.idx.esType,
            doc.extract[String]('PlaceID.?).get.getBytes(Charset.forName("UTF-8")),
            doc.toString.getBytes(Charset.forName("UTF-8"))
          )
        )*/
        }
        val reqSize = bulkRequest.numberOfActions()
        bulkRequest.execute(new ActionListener[BulkResponse] {
          override def onResponse(response: BulkResponse): Unit = {
            try {
              //esClient.admin().indices().prepareRefresh(indexParams.idx.index).execute().get()
              val failures = "[" + response.getItems.filter(_.isFailed).map(x => "{\"PlaceID\": \"" + x.getId + "\", \"error\": " + x.getFailureMessage.toJson.toString + "}").mkString(",") + "]"
              val success = "[" + response.getItems.filter(!_.isFailed).map(x => "\"" + x.getId + "\"").mkString(",") + "]"
              val resp = parse("{\"failed\": " + failures + ", \"successful\": " + success + "}")
              if (response.hasFailures) {
                val timeTaken = System.currentTimeMillis - indexParams.startTime
                warn("[indexing] [" + reqSize + "] [" + response.getTookInMillis + "/" + timeTaken + "] [" + response.buildFailureMessage() + "] [" + indexParams.req.clip.toString + "]->[" + indexParams.req.httpReq.uri + "]")
                completer ! IndexFailureResult(resp)
              }
              else {
                val timeTaken = System.currentTimeMillis - indexParams.startTime
                info("[indexed] [" + reqSize + "] [" + response.getTookInMillis + "/" + timeTaken + "] [" + response.getItems.take(10).map(x => x.getId).toList + "] [" + indexParams.req.clip.toString + "]->[" + indexParams.req.httpReq.uri + "]")
                completer ! IndexSuccessResult(resp)
              }
            } catch {
              case e: Throwable =>
                val timeTaken = System.currentTimeMillis - indexParams.startTime
                error("[indexing] [" + reqSize + "] [" + timeTaken + "] [" + e.getMessage + "] [" + indexParams.req.clip.toString + "]->[" + indexParams.req.httpReq.uri + "]", e)
                throw e
            }
          }

          override def onFailure(e: Throwable): Unit = {
            val timeTaken = System.currentTimeMillis - indexParams.startTime
            error("[indexing] [" + reqSize + "] [" + timeTaken + "] [" + e.getMessage + "] [" + indexParams.req.clip.toString + "]->[" + indexParams.req.httpReq.uri + "]", e)
            throw e
          }
        })
      } catch {
        case e: Throwable => throw e
      }
  }

}
