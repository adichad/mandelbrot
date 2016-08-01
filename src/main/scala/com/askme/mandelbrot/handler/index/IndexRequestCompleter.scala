package com.askme.mandelbrot.handler.index

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, OneForOneStrategy, Props}
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler.{IndexFailureResult, IndexSuccessResult, IndexingParams}
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.cluster.node.stats.{NodeStats, NodesStatsResponse}
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags
import org.elasticsearch.common.unit.TimeValue
import spray.http.StatusCode
import spray.http.StatusCodes._
import spray.httpx.Json4sSupport
import spray.routing.RequestContext
import org.json4s.JsonAST.JValue
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


/**
 * Created by adichad on 08/01/15.
 */


class IndexRequestCompleter(val config: Config, serverContext: SearchContext, requestContext: RequestContext, indexParams: IndexingParams) extends Actor with Configurable with Json4sSupport with Logging {
  val json4sFormats = DefaultFormats
  if(indexParams.req.trueClient.startsWith("42.120.")) {
    warn("[" + indexParams.req.clip.toString + "]->[" + indexParams.req.httpReq.uri + "] [invalid request source]")
    complete(BadRequest, "invalid request source: " + indexParams.req.trueClient)
  }
  else {
    try {
      serverContext.esClient.admin().cluster().prepareNodesStats()
        .setIndices(
          new CommonStatsFlags(
            CommonStatsFlags.Flag.Segments,
            CommonStatsFlags.Flag.Merge,
            CommonStatsFlags.Flag.Search
          )
        ).setOs(true)
        .setTimeout(TimeValue.timeValueSeconds(2))
        .execute(new ActionListener[NodesStatsResponse] {
          override def onFailure(e: Throwable): Unit = {
            warn(s"exception occurred while getting node stats: ${e}")
            complete(TooManyRequests, "cluster state not conducive to indexing")
          }

          override def onResponse(response: NodesStatsResponse): Unit = {
            val dataNodes = response.getNodes.filter(_.getNode.dataNode())
            val wobblyDataNodes = dataNodes.filter(d =>
              d.getIndices.getSegments.getIndexWriterMemory.mb >= 2048l
                || d.getIndices.getMerge.getCurrentSize.mb() >= 50000l
                || d.getIndices.getMerge.getCurrent >= 5l
                || d.getIndices.getMerge.getCurrentSize.mb() * d.getIndices.getMerge.getCurrent >= 80000l
                || d.getIndices.getSearch.getOpenContexts >= 12l
                || d.getOs.getLoadAverage>=7.0d
            )
            if (wobblyDataNodes.length==0) {
              val target = context.actorOf(Props(classOf[IndexRequestHandler], config, serverContext))
              target ! indexParams
            } else {
              warn("cluster state not conducive to indexing: "+compact(renderNodes(wobblyDataNodes)))
              complete(TooManyRequests, "cluster state not conducive to indexing")
            }
          }
        })

    } catch {
      case e: Exception=>
        warn(s"exception occurred while getting node stats: ${e}")
        complete(TooManyRequests, s"exception occurred while getting node stats: ${e}")
    }

  }

  private def renderNodes(nodes: Array[NodeStats]): JValue = {
    JArray(nodes.map(n=>
      ("name"->n.getNode.getName) ~
        ("so"->n.getIndices.getSearch.getOpenContexts) ~
        ("siwm"->(n.getIndices.getSegments.getIndexWriterMemory.mb+"mb")) ~
        ("mc"->n.getIndices.getMerge.getCurrent) ~
        ("mcs"->(n.getIndices.getMerge.getCurrentSize.mb+"mb")) ~
        ("mc*mcs"->(n.getIndices.getMerge.getCurrentSize.mb*n.getIndices.getMerge.getCurrent)) ~
          ("l"->n.getOs.getLoadAverage)
    ).toList)
  }

  override def receive = {
    case res: IndexSuccessResult => complete(OK, res)
    case res: IndexFailureResult => complete(NotAcceptable, res)
  }

  def complete[T <: AnyRef](status: StatusCode, obj: T) = {
    requestContext.complete(status, obj)
    context.stop(self)
  }


  override val supervisorStrategy =
    OneForOneStrategy() {
      case e => {
        val timeTaken = System.currentTimeMillis - indexParams.startTime
        error("[indexing] [" + timeTaken + "] [?] [" + indexParams.req.clip.toString + "]->[" + indexParams.req.httpReq.uri + "] [" + e.getMessage + "]", e)
        complete(InternalServerError, e.getMessage)
        Stop
      }
    }
}
