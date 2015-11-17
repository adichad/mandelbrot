package com.askme.mandelbrot.scripts

import java.util

import org.elasticsearch.index.fielddata.ScriptDocValues.Strings
import org.elasticsearch.script.{AbstractLongSearchScript, NativeScriptFactory}

/**
  * Created by adichad on 17/11/15.
  */
class DealComparator extends NativeScriptFactory {
  override def needsScores = false

  override def newScript(params: util.Map[String, AnyRef]) = {
    new DealComparatorScript
  }
}

class DealComparatorScript extends AbstractLongSearchScript {
  override def runAsLong: Long = {
    val sourceList = doc.get("DealSource.Name").asInstanceOf[Strings].getValues
    if(sourceList.contains("HotDeal"))
      2
    else if(sourceList.contains("AskMe Deals"))
      1
    else if(sourceList.contains("APL + Deal"))
      0
    else
      3
  }
}