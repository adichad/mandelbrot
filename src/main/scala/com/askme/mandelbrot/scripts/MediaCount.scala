package com.askme.mandelbrot.scripts

import java.lang.Cloneable
import java.util

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

class MediaCount extends NativeScriptFactory {

  override def newScript(params: util.Map[String, AnyRef]): ExecutableScript = {
    val index = params.get("index").asInstanceOf[String]
    val esType = params.get("type").asInstanceOf[String]
    val catkws = RootServer.uniqueVals(index, esType, "Product.categorykeywordsaggr", "Product.categorykeywordsexact", " ", 100000)
    new MediaCountScript(RootServer.defaultContext.esClient, index, esType, catkws)
  }
}

class MediaCountScript(private val esClient: Client, index: String, esType: String, catkws: Set[String]) extends AbstractExecutableScript with Logging {
  val vars = new util.HashMap[String, AnyRef]()

  override def setNextVar(name: String, value: AnyRef): Unit = {
    vars.put(name, value)
  }

  private def analyze(esClient: Client, index: String, field: String, text: String): Array[String] =
    new AnalyzeRequestBuilder(esClient.admin.indices, index, text).setField(field).get().getTokens.map(_.getTerm).toArray

  private def mapAttributes(ans: String, ckws: util.ArrayList[AnyRef]): Array[String] = {
    val exactAns = analyze(esClient, index, "Product.categorykeywordsexact", ans).mkString(" ")

    if(catkws.contains(exactAns))
      ckws.map(exactAns+" "+XContentMapValues.nodeStringValue(_, "")).toArray
    else
      Array(exactAns)
  }

  override def run(): AnyRef = {
    try {
      if (vars.containsKey("ctx") && vars.get("ctx").isInstanceOf[util.Map[String, AnyRef]]) {
        val ctx = vars.get("ctx").asInstanceOf[util.Map[String, AnyRef]]
        if (ctx.containsKey("_source") && ctx.get("_source").isInstanceOf[util.Map[String, AnyRef]]) {
          val source = ctx.get("_source").asInstanceOf[util.Map[String, AnyRef]]

          // media count
          val mediaCount: Int = source.get("Media").asInstanceOf[util.ArrayList[AnyRef]].size +
            (if (XContentMapValues.nodeStringValue(source.get("CompanyLogoURL"), "") == "") 0 else 1) +
            source.get("Product").asInstanceOf[util.ArrayList[AnyRef]]
              .map(p => XContentMapValues.nodeStringValue(p.asInstanceOf[util.Map[String, AnyRef]].get("imageurls"), "")).filter(_ != "").length
          source.put("MediaCount", new java.lang.Integer(mediaCount))

          // augment noisy attribute values with pkws
          source.get("Product").asInstanceOf[util.ArrayList[AnyRef]].foreach { p =>
            val catkwraw = p.asInstanceOf[util.Map[String, AnyRef]].get("categorykeywords")
            val cats = (if(catkwraw == null) new util.ArrayList[AnyRef] else catkwraw.asInstanceOf[util.ArrayList[AnyRef]])
            val catkws = new util.ArrayList[AnyRef](cats.filter(!XContentMapValues.nodeStringValue(_, "").trim.isEmpty))
            catkws.append(XContentMapValues.nodeStringValue(p.asInstanceOf[util.Map[String, AnyRef]].get("l3category"), ""))
            p.asInstanceOf[util.Map[String, AnyRef]].get("stringattribute").asInstanceOf[util.ArrayList[AnyRef]].foreach { a =>
              a.asInstanceOf[util.Map[String, AnyRef]].put("answerexact", new util.ArrayList[AnyRef](a.asInstanceOf[util.Map[String, AnyRef]].get("answer").asInstanceOf[util.ArrayList[AnyRef]].map(ans => mapAttributes(XContentMapValues.nodeStringValue(ans, ""), catkws)).flatten))
            }
          }

          // create analyzed doc-value fields
          source.put("LocationNameDocVal", analyze(esClient, index, "LocationNameExact", source.get("LocationName").asInstanceOf[String]).mkString(" "))

          val aliases =
            if (source.get("CompanyAliases") == null) new util.ArrayList[String]()
            else new util.ArrayList[AnyRef](source.get("CompanyAliases").asInstanceOf[util.ArrayList[AnyRef]].map(a => analyze(esClient, index, "CompanyAliasesExact", XContentMapValues.nodeStringValue(a, "")).mkString(" ")))
          source.put("CompanyAliasesDocVal", aliases)

          source.put("AreaDocVal", analyze(esClient, index, "AreaExact", source.get("Area").asInstanceOf[String]).mkString(" "))

          val areaSyns =
            if (source.get("AreaSynonyms") == null) new util.ArrayList[String]()
            else new util.ArrayList[AnyRef](source.get("AreaSynonyms").asInstanceOf[util.ArrayList[AnyRef]].map(a => analyze(esClient, index, "AreaSynonymsExact", XContentMapValues.nodeStringValue(a, "")).mkString(" ")))
          source.put("AreaSynonymsDocVal", areaSyns)
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
