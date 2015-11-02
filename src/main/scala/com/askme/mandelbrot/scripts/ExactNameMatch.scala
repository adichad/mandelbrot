package com.askme.mandelbrot.scripts

import java.util

import org.elasticsearch.index.fielddata.ScriptDocValues.Strings
import org.elasticsearch.script.{ExecutableScript, NativeScriptFactory, AbstractLongSearchScript}

import scala.collection.JavaConversions._


class ExactNameMatch extends NativeScriptFactory {

  override def newScript(params: util.Map[String, AnyRef]): ExecutableScript = {
    new ExactNameMatchScript(params.get("name").asInstanceOf[String])
  }

  override def needsScores = false
}

class ExactNameMatchScript(name: String) extends AbstractLongSearchScript {

  override def runAsLong: Long = {
    if(doc.get("LocationNameDocVal").asInstanceOf[Strings].getValues.contains(name) ||
      doc.get("CompanyAliasesDocVal").asInstanceOf[Strings].getValues.contains(name)) 0
    else if(doc.get("Product.l3categorydocval").asInstanceOf[Strings].getValues.contains(name) ||
      doc.get("Product.categorykeywordsdocval").asInstanceOf[Strings].getValues.contains(name) ||
      doc.get("Product.l1categorydocval").asInstanceOf[Strings].getValues.contains(name) ||
      doc.get("Product.l2categorydocval").asInstanceOf[Strings].getValues.contains(name)) 1
    else
      2
  }
}
