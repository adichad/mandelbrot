package com.askme.mandelbrot.scripts

import java.util

import org.elasticsearch.index.fielddata.ScriptDocValues.Strings
import org.elasticsearch.script.{AbstractLongSearchScript, ExecutableScript, NativeScriptFactory}
import scala.collection.JavaConversions._

/**
  * Created by adichad on 05/04/16.
  */
class DealPayComparator extends NativeScriptFactory {
  override def newScript(params: util.Map[String, AnyRef]): ExecutableScript =
    new DealChannelComparatorScript

  override def needsScores = false
}

class DealPayComparatorScript extends AbstractLongSearchScript {

  override def runAsLong: Long = {
    val sourceList = doc.get("DealSource.Name").asInstanceOf[Strings].getValues
    if(sourceList.contains("pay"))
      0
    else
      1
  }
}

