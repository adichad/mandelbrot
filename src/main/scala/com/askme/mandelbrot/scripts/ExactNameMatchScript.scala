package com.askme.mandelbrot.scripts

import java.util

import grizzled.slf4j.Logging
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.elasticsearch.index.fielddata.ScriptDocValues.Strings
import org.elasticsearch.script.{ExecutableScript, NativeScriptFactory, AbstractLongSearchScript}

import scala.collection.JavaConversions._


class ExactNameMatch extends NativeScriptFactory with Logging {

  override def newScript(params: util.Map[String, AnyRef]): ExecutableScript = {
    new ExactNameMatchScript(params.get("name").asInstanceOf[String])
  }
}

class ExactNameMatchScript(name: String) extends AbstractLongSearchScript with Logging {

  override def runAsLong: Long = {
    if(doc.get("LocationNameExact").asInstanceOf[Strings].getValues.filter(_ == name).size > 0 ||
      doc.get("CompanyAliasesExact").asInstanceOf[Strings].getValues.filter(_ == name).size > 0) 0
    else
      1
  }
}
