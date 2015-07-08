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
    new MediaCountSortScript(Array(1l, 2l, 5l, 15l, 21l))
  }
}

class MediaCountSortScript(buckets: Array[Long]) extends AbstractLongSearchScript {
  override def runAsLong: Long = {
    buckets.indexWhere(doc.get("MediaCount").asInstanceOf[ScriptDocValues.Longs].getValue <= _)
  }
}


