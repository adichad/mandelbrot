package com.askme.mandelbrot.scripts

import java.util

import org.elasticsearch.common.xcontent.support.XContentMapValues
import org.elasticsearch.script.{ExecutableScript, NativeScriptFactory, AbstractExecutableScript}
import scala.collection.JavaConversions._

/**
 * Created by adichad on 20/05/15.
 */

class MediaCount extends NativeScriptFactory {
  override def newScript(params: util.Map[String, AnyRef]): ExecutableScript = {
    new MediaCountScript
  }
}

class MediaCountScript extends AbstractExecutableScript {
  val vars = new util.HashMap[String, AnyRef]()

  override def setNextVar(name: String, value: AnyRef): Unit = {
    vars.put(name, value)
  }

  override def run(): AnyRef = {
    if (vars.containsKey("ctx") && vars.get("ctx").isInstanceOf[util.Map[String,AnyRef]]) {
      val ctx = vars.get("ctx").asInstanceOf[util.Map[String,AnyRef]]
      if (ctx.containsKey("_source") && ctx.get("_source").isInstanceOf[util.Map[String,AnyRef]]) {
        val source: util.Map[String, AnyRef] = ctx.get("_source").asInstanceOf[util.Map[String,AnyRef]]

        val count: Int = source.get("Media").asInstanceOf[util.ArrayList[AnyRef]].size +
          (if(XContentMapValues.nodeStringValue(source.get("CompanyLogoURL"), "") == "") 0 else 1) +
          source.get("Product").asInstanceOf[util.ArrayList[AnyRef]]
            .map(p=>XContentMapValues.nodeStringValue(p.asInstanceOf[util.Map[String, AnyRef]].get("imageurls"), "")).filter(_!="").length
        source.put("MediaCount", new java.lang.Integer(count))
      }
      // return the context
      return ctx
    }
    // shouldn't ever happen
    return null
  }
}
