package com.askme.mandelbrot.scripts

import java.util

import org.elasticsearch.index.fielddata.ScriptDocValues
import org.elasticsearch.index.fielddata.ScriptDocValues.Strings
import org.elasticsearch.script.{AbstractLongSearchScript, ExecutableScript, NativeScriptFactory}
import scala.collection.JavaConversions._

/**
 * Created by adichad on 06/02/15.
 */
class GeoBucket extends NativeScriptFactory {
  val buckets = Array(1.5d, 4d, 8d, 30d, Double.MaxValue)

  override def newScript(params: util.Map[String, AnyRef]): ExecutableScript = {

    new GeoBucketScript(
      params.get("lat").asInstanceOf[Double],
      params.get("lon").asInstanceOf[Double],
      params.get("areaSlugs").asInstanceOf[String].split("#").toSet,
      buckets)
  }
}

class GeoBucketScript(lat: Double, lon: Double, areas: Set[String], buckets: Array[Double]) extends AbstractLongSearchScript {
  override def runAsLong: Long = {
    val dist = doc.get("LatLong").asInstanceOf[ScriptDocValues.GeoPoints].distanceInKm(lat, lon)

    if(doc.get("AreaSlug").asInstanceOf[Strings].getValues.filter(areas.contains(_)).size>0)
      0
    else
      buckets.indexWhere(dist <= _)
  }
}
