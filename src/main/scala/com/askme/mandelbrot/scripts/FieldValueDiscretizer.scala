package com.askme.mandelbrot.scripts

import java.util

import org.elasticsearch.index.fielddata.ScriptDocValues
import org.elasticsearch.script._

/**
  * Created by adichad on 23/05/16.
  */
class FieldValueDiscretizer extends NativeScriptFactory {

  override def newScript(params: util.Map[String, AnyRef]): ExecutableScript = {
    params.getOrDefault("type", "String").asInstanceOf[String] match {
      case "Int" => new IntFieldDiscretizerScript(
        params.get("buckets").asInstanceOf[Array[Int]],
        params.get("field").asInstanceOf[String]
      )
      case "Long" => new LongFieldDiscretizerScript(
        params.get("buckets").asInstanceOf[Array[Long]],
        params.get("field").asInstanceOf[String]
      )
      case "Float" => new FloatFieldDiscretizerScript(
        params.get("buckets").asInstanceOf[Array[Float]],
        params.get("field").asInstanceOf[String]
      )
      case "Double" => new DoubleFieldDiscretizerScript(
        params.get("buckets").asInstanceOf[Array[Double]],
        params.get("field").asInstanceOf[String]
      )
      case _ => new StringFieldDiscretizerScript(
        params.get("buckets").asInstanceOf[Array[String]],
        params.get("field").asInstanceOf[String]
      )
    }

  }

  override def needsScores = false
}


class IntFieldDiscretizerScript(buckets: Array[Int], field: String) extends AbstractLongSearchScript {

  override def runAsLong: Long = {
    val v = doc.get(field).asInstanceOf[ScriptDocValues.Longs].getValue.toInt
    buckets.indexWhere(v <= _)
  }
}

class LongFieldDiscretizerScript(buckets: Array[Long], field: String) extends AbstractLongSearchScript {

  override def runAsLong: Long = {
    val v = doc.get(field).asInstanceOf[ScriptDocValues.Longs].getValue
    buckets.indexWhere(v <= _)
  }
}


class FloatFieldDiscretizerScript(buckets: Array[Float], field: String) extends AbstractLongSearchScript {

  override def runAsLong: Long = {
    val v = doc.get(field).asInstanceOf[ScriptDocValues.Doubles].getValue.toFloat
    buckets.indexWhere(v <= _)
  }
}


class DoubleFieldDiscretizerScript(buckets: Array[Double], field: String) extends AbstractLongSearchScript {

  override def runAsLong: Long = {
    val v = doc.get(field).asInstanceOf[ScriptDocValues.Doubles].getValue
    buckets.indexWhere(v <= _)
  }
}


class StringFieldDiscretizerScript(buckets: Array[String], field: String) extends AbstractLongSearchScript {

  override def runAsLong: Long = {
    val v = doc.get(field).asInstanceOf[ScriptDocValues.Strings].getValue
    buckets.indexWhere(v <= _)
  }
}
