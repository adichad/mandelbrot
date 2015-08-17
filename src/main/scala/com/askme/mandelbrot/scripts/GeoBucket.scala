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
      params.getOrDefault("coordfield", "LatLong").asInstanceOf[String],
      params.getOrDefault("areafield", "AreaDocVal").asInstanceOf[String],
      params.getOrDefault("synfield", "AreaSynonymsDocVal").asInstanceOf[String],
      params.getOrDefault("skufield", "SKUAreasDocVal").asInstanceOf[String],
      buckets)
  }
}

object GeoBucketScript {
  val empty = new util.ArrayList[String]
}
class GeoBucketScript(lat: Double, lon: Double, areas: Set[String], coordfield: String, areafield: String, synfield: String, skufield: String, buckets: Array[Double]) extends AbstractLongSearchScript {
  import GeoBucketScript._
  override def runAsLong: Long = {
    val mdoc = doc.asInstanceOf[util.Map[String, util.AbstractList[String]]]
    if(mdoc.getOrDefault(areafield, empty).asInstanceOf[Strings].getValues.exists(areas.contains(_)))
      0
    else if(mdoc.getOrDefault(synfield, empty).asInstanceOf[Strings].getValues.exists(areas.contains(_)))
      0
    else if(mdoc.getOrDefault(skufield, empty).asInstanceOf[Strings].getValues.exists(areas.contains(_)))
      0
    else
      buckets.indexWhere(
        (if(lat!=0||lon!=0) doc.get(coordfield).asInstanceOf[ScriptDocValues.GeoPoints].distanceInKm(lat, lon) else 100d) <= _)
  }
}
