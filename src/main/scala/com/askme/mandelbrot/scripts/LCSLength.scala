package com.askme.mandelbrot.scripts

import java.util

import org.elasticsearch.script.{AbstractFloatSearchScript, ExecutableScript, NativeScriptFactory}

/**
 * Created by adichad on 27/03/15.
 */
class LCSLength extends NativeScriptFactory {
  override def newScript(params: util.Map[String, AnyRef]): ExecutableScript = {
    new LCSLengthScript
  }
}

class LCSLengthScript extends AbstractFloatSearchScript {
  override def runAsFloat: Float = {
    score
  }
}
