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
import spray.json._


/**
 * Created by adichad on 19/03/15.
 */
class IndexRequestHandler(val config: Config, serverContext: SearchContext) extends Actor with Logging with Configurable {

  private val esClient = serverContext.esClient


  override def receive = {
    case indexParams: IndexingParams =>
      val completer = context.parent
      try {
        val json = parse(indexParams.data.data)

        val idField = string("mappings."+indexParams.idx.esType+".id")
        val pipers = actors(context, "mappings."+indexParams.idx.esType+".pipers")
        val bulkRequest = esClient.prepareBulk
        for (doc: JValue <- json.children) {
          val idraw = doc \ idField
          val id: String = string("mappings."+indexParams.idx.esType+".idType").toLowerCase match {
            case "int" => idraw.asInstanceOf[JInt].values.toString
            case "string" => idraw.asInstanceOf[JString].values
            case _ => idraw.asInstanceOf[JString].values
          }
          bulkRequest.add(
            esClient.prepareIndex(indexParams.idx.index, indexParams.idx.esType, id)
              .setSource(compact(render(doc)))
          )
        }
        pipers.foreach(_ ! json)
        val reqSize = bulkRequest.numberOfActions()
        bulkRequest.execute(new ActionListener[BulkResponse] {
          override def onResponse(response: BulkResponse): Unit = {
            try {
              val failures = "[" + response.getItems.filter(_.isFailed).map(x => "{\""+idField+"\": \"" + x.getId + "\", \"error\": " + x.getFailureMessage.toJson.toString + "}").mkString(",") + "]"
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
