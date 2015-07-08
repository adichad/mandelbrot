package com.askme.mandelbrot.scripts

import java.util

import org.elasticsearch.index.fielddata.ScriptDocValues.Strings
import org.elasticsearch.script.{ExecutableScript, NativeScriptFactory, AbstractLongSearchScript}

import scala.collection.JavaConversions._


class ExactNameMatch extends NativeScriptFactory {

  override def newScript(params: util.Map[String, AnyRef]): ExecutableScript = {
    new ExactNameMatchScript(params.get("name").asInstanceOf[String])
  }
}

class ExactNameMatchScript(name: String) extends AbstractLongSearchScript {

  override def runAsLong: Long = {
    if(doc.get("LocationNameDocVal").asInstanceOf[Strings].getValues.exists(_==name) ||
      doc.get("CompanyAliasesDocVal").asInstanceOf[Strings].getValues.exists(_==name)) 0
    else
      1
  }
}
