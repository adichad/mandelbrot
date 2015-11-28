package com.askme.mandelbrot.scripts

import java.util

import org.elasticsearch.index.fielddata.ScriptDocValues
import org.elasticsearch.index.fielddata.ScriptDocValues.Strings
import org.elasticsearch.script.{AbstractLongSearchScript, ExecutableScript, NativeScriptFactory}

/**
 * Created by adichad on 20/05/15.
 */



class MediaCountSort extends NativeScriptFactory {
  override def newScript(params: util.Map[String, AnyRef]): ExecutableScript = {
    new MediaCountSortScript(Array(0l, 1l, 2l, 4l, 9l, 19l, Long.MaxValue))
  }
  override def needsScores = false
}

class MediaCountSortScript(buckets: Array[Long]) extends AbstractLongSearchScript {
  override def runAsLong: Long = {
    val count = doc.get("MediaCount").asInstanceOf[ScriptDocValues.Longs].getValue
    buckets.indexWhere(count <= _)
  }
}

