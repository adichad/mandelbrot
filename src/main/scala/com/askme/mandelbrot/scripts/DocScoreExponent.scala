package com.askme.mandelbrot.scripts

import java.util

import org.elasticsearch.script.{AbstractLongSearchScript, AbstractFloatSearchScript, ExecutableScript, NativeScriptFactory}

/**
 * Created by adichad on 22/08/15.
 */

class DocScoreExponent extends NativeScriptFactory {
  override def newScript(params: util.Map[String, AnyRef]): ExecutableScript = {
    new DocScoreExponentScript
  }

  override def needsScores = false
}

class DocScoreExponentScript extends AbstractLongSearchScript {
  override def runAsLong: Long = java.lang.Math.getExponent(score)
}