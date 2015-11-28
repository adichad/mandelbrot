package com.askme.mandelbrot.scripts

import java.util

import org.elasticsearch.index.fielddata.ScriptDocValues.Strings
import org.elasticsearch.script.{AbstractLongSearchScript, ExecutableScript, NativeScriptFactory}
import scala.collection.JavaConversions._

class DealChannelComparator extends NativeScriptFactory {
  override def newScript(params: util.Map[String, AnyRef]): ExecutableScript =
    new DealChannelComparatorScript

  override def needsScores = false
}

class DealChannelComparatorScript extends AbstractLongSearchScript {

  override def runAsLong: Long = {
    if(doc.containsKey("ApplicableTo") &&
      doc.get("ApplicableTo").asInstanceOf[Strings].getValues.exists(_.trim.nonEmpty))
      0
    else
      1
  }
}

