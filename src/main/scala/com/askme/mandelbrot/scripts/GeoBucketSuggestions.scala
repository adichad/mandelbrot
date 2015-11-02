package com.askme.mandelbrot.scripts

import java.util

import org.elasticsearch.index.fielddata.ScriptDocValues
import org.elasticsearch.index.fielddata.ScriptDocValues.Strings
import org.elasticsearch.script.{AbstractLongSearchScript, ExecutableScript, NativeScriptFactory}
import scala.collection.JavaConversions._

/**
 * Created by adichad on 21/08/15.
 */
class GeoBucketSuggestions extends NativeScriptFactory {
  val buckets = Array(1.5d, 4d, 8d, 30d, Double.MaxValue)

  override def newScript(params: util.Map[String, AnyRef]): ExecutableScript = {

    new GeoBucketSuggestionsScript(
      params.get("lat").asInstanceOf[Double],
      params.get("lon").asInstanceOf[Double],
      params.get("areas").asInstanceOf[String].split("#").toSet,
      buckets)
  }

  override def needsScores = false
}

object GeoBucketSuggestionsScript {
  val empty = new util.ArrayList[String]
}
class GeoBucketSuggestionsScript(lat: Double, lon: Double, areas: Set[String], buckets: Array[Double]) extends AbstractLongSearchScript {
  import GeoBucketScript._
  override def runAsLong: Long = {
    val mdoc = doc.asInstanceOf[util.Map[String, util.AbstractList[String]]]
    if(mdoc.getOrDefault("targeting.areadocval", empty).asInstanceOf[Strings].getValues.exists(areas.contains))
      0
    else
      buckets.indexWhere(
        (if(lat!=0||lon!=0) doc.get("targeting.coordinates").asInstanceOf[ScriptDocValues.GeoPoints].distanceInKm(lat, lon) else 100d) <= _)
  }
}
