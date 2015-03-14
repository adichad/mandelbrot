package com.askme.mandelbrot.scripts

import java.util

import org.elasticsearch.script.{AbstractFloatSearchScript, ExecutableScript, NativeScriptFactory}

/**
 * Created by adichad on 14/03/15.
 */
class DocScore extends NativeScriptFactory {
  override def newScript(params: util.Map[String, AnyRef]): ExecutableScript = {
    new DocScoreScript
  }
}

class DocScoreScript extends AbstractFloatSearchScript {
  override def runAsFloat: Float = score
}
