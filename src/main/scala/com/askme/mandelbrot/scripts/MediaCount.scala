package com.askme.mandelbrot.scripts

import java.lang.Cloneable
import java.util

import com.askme.mandelbrot.server.RootServer
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
    new MediaCountScript(RootServer.defaultContext.esClient, index, esType)
  }
}

class MediaCountScript(private val esClient: Client, index: String, esType: String) extends AbstractExecutableScript {
  val vars = new util.HashMap[String, AnyRef]()
  val search = esClient.prepareExists(index).setTypes(esType)

  override def setNextVar(name: String, value: AnyRef): Unit = {
    vars.put(name, value)
  }

  private def analyze(esClient: Client, index: String, field: String, text: String): Array[String] =
    new AnalyzeRequestBuilder(esClient.admin.indices, index, text).setField(field).get().getTokens.map(_.getTerm).toArray

  private def mapAttributes(ans: String, ckws: util.ArrayList[String]): Array[String] = {
    val exactAns = analyze(esClient, index, "Product.categorykeywordsexact", ans).mkString(" ")

    if(search.setQuery(termQuery("Product.categorykeywordsexact",exactAns)).execute().get().exists())
      ckws.map(exactAns+" "+_).toArray
    else
      Array(exactAns)
  }

  override def run(): AnyRef = {
    if (vars.containsKey("ctx") && vars.get("ctx").isInstanceOf[util.Map[String,AnyRef]]) {
      val ctx = vars.get("ctx").asInstanceOf[util.Map[String,AnyRef]]
      if (ctx.containsKey("_source") && ctx.get("_source").isInstanceOf[util.Map[String,AnyRef]]) {
        val source = ctx.get("_source").asInstanceOf[util.Map[String,AnyRef]]

        // media count
        val mediaCount: Int = source.get("Media").asInstanceOf[util.ArrayList[AnyRef]].size +
          (if(XContentMapValues.nodeStringValue(source.get("CompanyLogoURL"), "") == "") 0 else 1) +
          source.get("Product").asInstanceOf[util.ArrayList[AnyRef]]
            .map(p=>XContentMapValues.nodeStringValue(p.asInstanceOf[util.Map[String, AnyRef]].get("imageurls"), "")).filter(_!="").length
        source.put("MediaCount", new java.lang.Integer(mediaCount))

        // augment noisy attribute values with pkws
        source.get("Product").asInstanceOf[util.ArrayList[util.Map[String,AnyRef]]].foreach { p=>
          val catkws = new util.ArrayList[String](p.get("categorykeywords").asInstanceOf[util.ArrayList[String]].filter(!_.trim.isEmpty))
          catkws.append(p.get("l3category").asInstanceOf[String])
          p.get("stringattribute").asInstanceOf[util.ArrayList[util.Map[String, AnyRef]]].foreach { a =>
            a.put("answerexact", new util.ArrayList[String](a.get("answer").asInstanceOf[util.ArrayList[String]].map(mapAttributes(_, catkws)).flatten))
          }
        }

        // create analyzed doc-value fields
        source.put("LocationNameDocVal", analyze(esClient, index, "LocationNameExact", source.get("LocationName").asInstanceOf[String]).mkString(" "))
        val aliases =
          if(source.get("CompanyAliases")==null) new util.ArrayList[String]()
          else new util.ArrayList[String](source.get("CompanyAlaises").asInstanceOf[util.ArrayList[String]].map(a=>analyze(esClient, index, "CompanyAliasesExact", a).mkString(" ")))
        source.put("CompanyAliasesDocVal", aliases)

      }
      // return the context
      return ctx
    }
    // shouldn't ever happen
    return null
  }
}
