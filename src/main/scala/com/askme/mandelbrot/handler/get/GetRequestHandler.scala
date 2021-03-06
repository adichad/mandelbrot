package com.askme.mandelbrot.handler.get

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.EmptyResponse
import com.askme.mandelbrot.handler.get.message.GetParams
import com.askme.mandelbrot.handler.message.ErrorResponse
import com.askme.mandelbrot.handler.search.message.{GetResult, SuggestResult}
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.client.Client
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization



/**
 * Created by adichad on 08/01/15.
 */


class GetRequestHandler(val parentPath: String, serverContext: SearchContext) extends Actor with Configurable with Logging {
  private val esClient: Client = serverContext.esClient

  implicit val formats = Serialization.formats(NoTypeHints)


  override def receive = {

    case getParams: GetParams =>
      import getParams._
      try {
        val getter =
          esClient
            .prepareGet(string(s"mappings.$esType.index"), esType, id)
            .setFetchSource(select, null).setRealtime(true).setTransformSource(transform)
            .execute(new ActionListener[GetResponse] {
              override def onResponse(response: GetResponse): Unit = {
                if(response.isExists) {
                  val res = parse(response.getSourceAsString)
                  val endTime = System.currentTimeMillis
                  val timeTaken = endTime - startTime
                  info("[" + timeTaken + "] [" + clip.toString + "]->[" + req.uri + "]")
                  context.parent ! GetResult(timeTaken, response.getVersion, response.getIndex, res)
                }
                else {
                  val endTime = System.currentTimeMillis
                  val timeTaken = endTime - startTime
                  info("[" + timeTaken + "] [" + clip.toString + "]->[" + req.uri + "]")
                  context.parent ! EmptyResponse("resource not found")
                }
              }

              override def onFailure(e: Throwable): Unit = {
                val timeTaken = System.currentTimeMillis() - startTime
                error("[" + timeTaken + "] ["+e.getMessage+"] [" + clip.toString + "]->[" + req.uri + "]", e)
                context.parent ! ErrorResponse(e.getMessage, e)
              }

            })
      } catch {
        case e: Throwable =>
          val timeTaken = System.currentTimeMillis() - startTime
          error("[" + timeTaken + "] ["+e.getMessage+"] [" + clip.toString + "]->[" + req.uri + "]", e)
          context.parent ! ErrorResponse(e.getMessage, e)
      }

  }

}

