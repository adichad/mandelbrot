package com.askme.mandelbrot.scripts

import org.elasticsearch.index.fielddata.ScriptDocValues
import org.elasticsearch.script.{AbstractLongSearchScript, ExecutableScript, NativeScriptFactory}
import java.util

/**
  * Created by adichad on 26/02/16.
  */
class GeoDistanceBucket extends NativeScriptFactory {
  val buckets = Array(1.5d, 4d, 8d, 30d, Double.MaxValue)

  override def newScript(params: util.Map[String, AnyRef]): ExecutableScript = {

    new GeoDistanceBucketScript(
      params.get("lat").asInstanceOf[Double],
      params.get("lon").asInstanceOf[Double],
      params.getOrDefault("coordfield", "center").asInstanceOf[String],
      buckets)
  }

  override def needsScores = false
}

class GeoDistanceBucketScript(lat: Double, lon: Double, coordfield: String, buckets: Array[Double]) extends AbstractLongSearchScript {
  override def runAsLong: Long = {
    val distance =
      if (lat != 0 || lon != 0)
        doc.get(coordfield).asInstanceOf[ScriptDocValues.GeoPoints].distanceInKmWithDefault(lat, lon, 100d)
      else
        100d
    buckets.indexWhere(distance <= _)
  }
}