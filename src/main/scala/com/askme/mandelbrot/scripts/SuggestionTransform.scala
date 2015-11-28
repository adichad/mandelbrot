package com.askme.mandelbrot.scripts

import java.util

import com.askme.mandelbrot.server.RootServer
import grizzled.slf4j.Logging
import org.elasticsearch.action.admin.indices.analyze.{AnalyzeAction, AnalyzeRequestBuilder}
import org.elasticsearch.client.Client
import org.elasticsearch.common.xcontent.support.XContentMapValues
import org.elasticsearch.script.{ExecutableScript, NativeScriptFactory, AbstractExecutableScript}
import scala.collection.JavaConversions._

/**
 * Created by adichad on 20/05/15.
 */

class SuggestionTransform extends NativeScriptFactory {

  override def newScript(params: util.Map[String, AnyRef]): ExecutableScript = {
    val index = params.get("index").asInstanceOf[String]
    val esType = params.get("type").asInstanceOf[String]
    new SuggestionTransformScript(RootServer.defaultContext.esClient, index, esType)
  }

  override def needsScores = false
}

class SuggestionTransformScript(private val esClient: Client, index: String, esType: String) extends AbstractExecutableScript with Logging {
  val vars = new util.HashMap[String, AnyRef]()

  override def setNextVar(name: String, value: AnyRef): Unit = {
    vars.put(name, value)
  }

  private def analyze(esClient: Client, index: String, field: String, text: String): Array[String] =
    new AnalyzeRequestBuilder(esClient.admin.indices, AnalyzeAction.INSTANCE, index, text).setField(field).get().getTokens.map(_.getTerm).toArray

  override def run(): AnyRef = {
    try {
      if (vars.containsKey("ctx")) {
        val ctx = vars.get("ctx").asInstanceOf[util.Map[String, AnyRef]]
        if (ctx.containsKey("_source")) {
          val source = ctx.get("_source").asInstanceOf[util.Map[String, AnyRef]]

          source.get("targeting").asInstanceOf[util.ArrayList[AnyRef]].foreach { t =>
            val target = t.asInstanceOf[util.Map[String, AnyRef]]
            if(target.containsKey("area"))
              target.put("areadocval",
                new util.ArrayList[String](target.get("area").asInstanceOf[util.ArrayList[AnyRef]]
                  .map(a=>analyze(esClient, index, "targeting.area", XContentMapValues.nodeStringValue(a, "")).mkString(" ")))
              )
          }
        }
        // return the context
        return ctx
      }
      // shouldn't ever happen
      null
    } catch {
      case e: Throwable => error(e.getMessage, e)
        throw e
    }
  }
}
