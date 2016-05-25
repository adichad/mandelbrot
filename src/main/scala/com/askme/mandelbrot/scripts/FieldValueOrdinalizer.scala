package com.askme.mandelbrot.scripts

import java.util

import org.elasticsearch.index.fielddata.ScriptDocValues
import org.elasticsearch.script.{AbstractLongSearchScript, ExecutableScript, NativeScriptFactory}

/**
  * Created by adichad on 25/05/16.
  */
class FieldValueOrdinalizer extends NativeScriptFactory {

  override def newScript(params: util.Map[String, AnyRef]): ExecutableScript = {
    params.getOrDefault("type", "String").asInstanceOf[String] match {
      case "Int" => new IntFieldOrdinalizerScript(
        params.get("field").asInstanceOf[String],
        params.get("mapper").asInstanceOf[PartialFunction[Int, Long]],
        params.get("missing").asInstanceOf[Long]
      )
      case "Long" => new LongFieldOrdinalizerScript(
        params.get("field").asInstanceOf[String],
        params.get("mapper").asInstanceOf[PartialFunction[Long, Long]],
        params.get("missing").asInstanceOf[Long]
      )
      case "Float" => new FloatFieldOrdinalizerScript(
        params.get("field").asInstanceOf[String],
        params.get("mapper").asInstanceOf[PartialFunction[Float, Long]],
        params.get("missing").asInstanceOf[Long]
      )
      case "Double" => new DoubleFieldOrdinalizerScript(
        params.get("field").asInstanceOf[String],
        params.get("mapper").asInstanceOf[PartialFunction[Double, Long]],
        params.get("missing").asInstanceOf[Long]
      )
      case _ => new StringFieldOrdinalizerScript(
        params.get("field").asInstanceOf[String],
        params.get("mapper").asInstanceOf[PartialFunction[String, Long]],
        params.get("missing").asInstanceOf[Long]
      )
    }

  }

  override def needsScores = false
}



class IntFieldOrdinalizerScript(field: String, mapper: PartialFunction[Int, Long], missing: Long) extends AbstractLongSearchScript {

  override def runAsLong: Long = {
    val fieldValue = doc.get(field).asInstanceOf[ScriptDocValues.Longs].getValue.toInt
    if(mapper isDefinedAt fieldValue) mapper apply fieldValue else missing
  }
}

class LongFieldOrdinalizerScript(field: String, mapper: PartialFunction[Long, Long], missing: Long) extends AbstractLongSearchScript {

  override def runAsLong: Long = {
    val fieldValue = doc.get(field).asInstanceOf[ScriptDocValues.Longs].getValue
    if(mapper isDefinedAt fieldValue) mapper apply fieldValue else missing
  }
}

class FloatFieldOrdinalizerScript(field: String, mapper: PartialFunction[Float, Long], missing: Long) extends AbstractLongSearchScript {

  override def runAsLong: Long = {
    val fieldValue = doc.get(field).asInstanceOf[ScriptDocValues.Doubles].getValue.toFloat
    if(mapper isDefinedAt fieldValue) mapper apply fieldValue else missing
  }
}

class DoubleFieldOrdinalizerScript(field: String, mapper: PartialFunction[Double, Long], missing: Long) extends AbstractLongSearchScript {

  override def runAsLong: Long = {
    val fieldValue = doc.get(field).asInstanceOf[ScriptDocValues.Doubles].getValue
    if(mapper isDefinedAt fieldValue) mapper apply fieldValue else missing
  }
}

class StringFieldOrdinalizerScript(field: String, mapper: PartialFunction[String, Long], missing: Long) extends AbstractLongSearchScript {

  override def runAsLong: Long = {
    val fieldValue = doc.get(field).asInstanceOf[ScriptDocValues.Strings].getValue
    if(mapper isDefinedAt fieldValue) mapper apply fieldValue else missing
  }
}