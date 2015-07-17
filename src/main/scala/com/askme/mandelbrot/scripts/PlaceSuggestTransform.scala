package com.askme.mandelbrot.scripts

import java.lang.Cloneable
import java.util

import com.askme.mandelbrot.server.RootServer
import com.askme.mandelbrot.server.RootServer
import grizzled.slf4j.Logging
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.client.Client
import org.elasticsearch.common.xcontent.support.XContentMapValues
import org.elasticsearch.script.{ExecutableScript, NativeScriptFactory, AbstractExecutableScript}
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Created by adichad on 20/05/15.
 */

class PlaceSuggestTransform extends NativeScriptFactory {

  override def newScript(params: util.Map[String, AnyRef]): ExecutableScript = {
    val index = params.get("index").asInstanceOf[String]
    val esType = params.get("type").asInstanceOf[String]
    new PlaceSuggestTransformScript(RootServer.defaultContext.esClient, index, esType)
  }
}

class PlaceSuggestTransformScript(private val esClient: Client, index: String, esType: String) extends AbstractExecutableScript with Logging {
  val vars = new util.HashMap[String, AnyRef]()

  override def setNextVar(name: String, value: AnyRef): Unit = {
    vars.put(name, value)
  }

  private def analyze(esClient: Client, index: String, field: String, text: String): Array[String] =
    new AnalyzeRequestBuilder(esClient.admin.indices, index, text).setField(field).get().getTokens.map(_.getTerm).toArray

  override def run(): AnyRef = {
    try {
      if (vars.containsKey("ctx") && vars.get("ctx").isInstanceOf[util.Map[String, AnyRef]]) {
        val ctx = vars.get("ctx").asInstanceOf[util.Map[String, AnyRef]]
        if (ctx.containsKey("_source") && ctx.get("_source").isInstanceOf[util.Map[String, AnyRef]]) {
          val source = ctx.get("_source").asInstanceOf[util.Map[String, AnyRef]]

          source.get("targeting").asInstanceOf[util.ArrayList[AnyRef]].foreach { t =>
            val target = t.asInstanceOf[util.Map[String, AnyRef]]
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
      return null
    } catch {
      case e: Throwable => error(e.getMessage, e)
        throw e
    }
  }
}
