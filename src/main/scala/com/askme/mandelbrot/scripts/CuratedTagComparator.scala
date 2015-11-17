package com.askme.mandelbrot.scripts

import java.util

import grizzled.slf4j.Logging
import org.elasticsearch.index.fielddata.ScriptDocValues.Strings
import org.elasticsearch.script.{AbstractLongSearchScript, ExecutableScript, NativeScriptFactory}
import scala.collection.JavaConversions._

/**
 * Created by adichad on 18/08/15.
 */
class CuratedTagComparator extends NativeScriptFactory {
  override def newScript(params: util.Map[String, AnyRef]): ExecutableScript = {
    new CuratedTagComparatorScript(
      params.get("shingles").asInstanceOf[String].split("#").toSet)
  }

  override def needsScores = false
}

object CuratedTagComparatorScript {
  val empty = new util.ArrayList[String]
}
class CuratedTagComparatorScript(shingles: Set[String]) extends AbstractLongSearchScript {
  import CuratedTagComparatorScript._
  override def runAsLong: Long = {
    val tags = doc.asInstanceOf[util.Map[String, util.AbstractList[String]]].getOrDefault("CuratedTagsDocVal", empty).asInstanceOf[Strings].getValues
    if(tags.exists(shingles.contains))
      2
    else if(!tags.isEmpty)
      1
    else
      0
  }
}

