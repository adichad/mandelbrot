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
      val charset = (if(indexParams.data.detected) "[detected " else "[declared ") + indexParams.data.source_charset+"]"

      try {
        val json = parse(indexParams.data.data)

        val idField = string("mappings."+indexParams.idx.esType+".id")
        val piperseq = pipers("mappings."+indexParams.idx.esType+".pipers")
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

        val reqSize = bulkRequest.numberOfActions()
        if(reqSize>0) {
          bulkRequest.execute(new ActionListener[BulkResponse] {
            override def onResponse(response: BulkResponse): Unit = {
              val failures = "[" + response.getItems.filter(_.isFailed).map(x => "{\"" + idField + "\": \"" + x.getId + "\", \"error\": " + x.getFailureMessage.toJson.toString + "}").mkString(",") + "]"
              val success = "[" + response.getItems.filter(!_.isFailed).map(x => "\"" + x.getId + "\"").mkString(",") + "]"
              val all = "[" + response.getItems.map(x => "\"" + x.getId + "\"").mkString(",") + "]"
              val respStr = "{\"failed\": " + failures + ", \"successful\": " + success + "}"
              val respAllFailedStr = "{\"failed\": " + all + ", \"successful\": []}"
              val respAllFailed = parse(respAllFailedStr)
              val resp = parse(respStr)
              try {
                if (response.hasFailures) {
                  val timeTaken = System.currentTimeMillis - indexParams.startTime
                  warn("[indexing] [" + response.getTookInMillis + "/" + timeTaken + "] [" + reqSize + "] [" + indexParams.req.clip.toString + "]->[" + indexParams.req.httpReq.uri + "] [" + response.buildFailureMessage() + "] " + charset + " [" + respStr + "]")
                  completer ! IndexFailureResult(resp)
                }
                else {
                  val timeTaken = System.currentTimeMillis - indexParams.startTime
                  info("[indexed] [" + response.getTookInMillis + "/" + timeTaken + "] [" + reqSize + "] [" + indexParams.req.clip.toString + "]->[" + indexParams.req.httpReq.uri + "] " + charset + " [" + respStr + "]")
                  piperseq.foreach(_.pipe(json, completer))
                  if (piperseq.isEmpty)
                    completer ! IndexSuccessResult(resp)
                }
              } catch {
                case e: Throwable =>
                  val timeTaken = System.currentTimeMillis - indexParams.startTime
                  error("[indexing] [" + timeTaken + "] [" + reqSize + "] [" + indexParams.req.clip.toString + "]->[" + indexParams.req.httpReq.uri + "] " + charset + " [" + e.getMessage + "]", e)
                  completer ! IndexFailureResult(respAllFailed)
              }
            }

            override def onFailure(e: Throwable): Unit = {
              val timeTaken = System.currentTimeMillis - indexParams.startTime
              error("[indexing] [" + timeTaken + "] [" + reqSize + "] [" + indexParams.req.clip.toString + "]->[" + indexParams.req.httpReq.uri + "] " + charset + " [" + e.getMessage + "]", e)
              completer ! IndexFailureResult(JString("batch failed with exception: " + e.toString))
            }
          })
        } else {
          val timeTaken = System.currentTimeMillis - indexParams.startTime
          error("[indexing] [" + timeTaken + "] [" + reqSize + "] [" + indexParams.req.clip.toString + "]->[" + indexParams.req.httpReq.uri + "] " + charset + " [empty indexing batch: nothing to do]")
          completer ! IndexFailureResult(JString("empty indexing batch: nothing to do"))
        }
      } catch {
        case e: Throwable =>
          val timeTaken = System.currentTimeMillis - indexParams.startTime
          error("[indexing] [" + timeTaken + "] [?] [" + indexParams.req.clip.toString + "]->[" + indexParams.req.httpReq.uri + "] "+charset+" [" + e.getMessage + "]", e)
          completer ! IndexFailureResult(JString("batch failed with exception: "+e.toString))
      }
  }

}
