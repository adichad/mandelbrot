package com.askme.mandelbrot.scripts

import java.util

import org.elasticsearch.index.fielddata.ScriptDocValues
import org.elasticsearch.script.{AbstractLongSearchScript, ExecutableScript, NativeScriptFactory}

/**
 * Created by adichad on 06/02/15.
 */
class GeoBucket extends NativeScriptFactory {
  val buckets = Array(1.5d, 4d, 8d, 30d, Double.MaxValue)

  override def newScript(params: util.Map[String, AnyRef]): ExecutableScript = {
    new GeoBucketScript(params.get("lat").asInstanceOf[Double], params.get("lon").asInstanceOf[Double], buckets)
  }
}

class GeoBucketScript(lat: Double, lon: Double, buckets: Array[Double]) extends AbstractLongSearchScript {
  override def runAsLong: Long = {
    val dist = doc.get("LatLong").asInstanceOf[ScriptDocValues.GeoPoints].distanceInKm(lat, lon)
    buckets.indexWhere(dist <= _)
  }
}
