package com.askme.mandelbrot.handler.index

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.{IndexFailureResult, IndexSuccessResult, IndexingParams}
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.BulkResponse
import org.json4s._
import org.json4s.jackson.JsonMethods._
import spray.json.DefaultJsonProtocol._
import spray.json.{CompactPrinter, JsArray, JsValue, JsonParser, _}


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
              val respStr = "{\"failed\": " + failures + ", \"successful\": " + success + "}"
              val resp = parse(respStr)
              if (response.hasFailures) {
                val timeTaken = System.currentTimeMillis - indexParams.startTime
                warn("[indexing] [" + response.getTookInMillis + "/" + timeTaken + "] [" + reqSize + "] [" + indexParams.req.clip.toString + "]->[" + indexParams.req.httpReq.uri + "] [" + response.buildFailureMessage() + "] [" + respStr + "]")
                completer ! IndexFailureResult(resp)
              }
              else {
                val timeTaken = System.currentTimeMillis - indexParams.startTime
                info("[indexed] [" + response.getTookInMillis + "/" + timeTaken + "] [" + reqSize + "] [" + indexParams.req.clip.toString + "]->[" + indexParams.req.httpReq.uri + "] [" + respStr + "]")
                completer ! IndexSuccessResult(resp)
              }
            } catch {
              case e: Throwable =>
                val timeTaken = System.currentTimeMillis - indexParams.startTime
                error("[indexing] [" + timeTaken + "] [" + reqSize + "] [" + indexParams.req.clip.toString + "]->[" + indexParams.req.httpReq.uri + "] [" + e.getMessage + "]", e)
                throw e
            }
          }

          override def onFailure(e: Throwable): Unit = {
            val timeTaken = System.currentTimeMillis - indexParams.startTime
            error("[indexing] [" + timeTaken + "] [" + reqSize + "] [" + indexParams.req.clip.toString + "]->[" + indexParams.req.httpReq.uri + "] [" + e.getMessage + "]", e)
            throw e
          }
        })
      } catch {
        case e: Throwable => throw e
      }
  }

}
