package com.askme.mandelbrot.scripts

import java.util

import org.elasticsearch.index.fielddata.ScriptDocValues
import org.elasticsearch.script.{AbstractLongSearchScript, ExecutableScript, NativeScriptFactory}
import scala.collection.JavaConversions._
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
  * Created by adichad on 25/05/16.
  */
class FieldValueOrdinalizer extends NativeScriptFactory {

  override def newScript(params: util.Map[String, AnyRef]): ExecutableScript = {
    implicit val formats = org.json4s.DefaultFormats
    params.getOrDefault("type", "String").asInstanceOf[String] match {
      case "Int" => new IntFieldOrdinalizerScript(
        params.get("field").asInstanceOf[String],
        parse(params.get("mapper").asInstanceOf[String]).extract[Map[Int,Long]].map(x=>(x._1.toInt, x._2)),
        params.get("missing").asInstanceOf[Long]
      )
      case "Long" => new LongFieldOrdinalizerScript(
        params.get("field").asInstanceOf[String],
        parse(params.get("mapper").asInstanceOf[String]).extract[Map[Long,Long]].map(x=>(x._1.toLong, x._2)),
        params.get("missing").asInstanceOf[Long]
      )
      case "Float" => new FloatFieldOrdinalizerScript(
        params.get("field").asInstanceOf[String],
        parse(params.get("mapper").asInstanceOf[String]).extract[Map[Float,Long]].map(x=>(x._1.toFloat, x._2)),
        params.get("missing").asInstanceOf[Long]
      )
      case "Double" => new DoubleFieldOrdinalizerScript(
        params.get("field").asInstanceOf[String],
        parse(params.get("mapper").asInstanceOf[String]).extract[Map[String,Long]].map(x=>(x._1.toDouble, x._2)),
        params.get("missing").asInstanceOf[Long]
      )
      case _ => new StringFieldOrdinalizerScript(
        params.get("field").asInstanceOf[String],
        parse(params.get("mapper").asInstanceOf[String]).extract[Map[String,Int]].map(x=>(x._1, x._2.toLong)),
        params.get("missing").asInstanceOf[Int].toLong
      )
    }

  }

  override def needsScores = false
}



class IntFieldOrdinalizerScript(field: String, mapper: PartialFunction[Int, Long], missing: Long) extends AbstractLongSearchScript {

  override def runAsLong: Long = {
    val fieldValue = doc.get(field).asInstanceOf[ScriptDocValues.Longs].getValues.map(_.toInt).filter(mapper.isDefinedAt)
    if(fieldValue.nonEmpty)
      mapper apply fieldValue.head
    else missing
  }
}

class LongFieldOrdinalizerScript(field: String, mapper: PartialFunction[Long, Long], missing: Long) extends AbstractLongSearchScript {

  override def runAsLong: Long = {
    val fieldValue = doc.get(field).asInstanceOf[ScriptDocValues.Longs].getValues.filter(mapper isDefinedAt _)
    if(fieldValue.nonEmpty)
      mapper apply fieldValue.head
    else missing
  }
}

class FloatFieldOrdinalizerScript(field: String, mapper: PartialFunction[Float, Long], missing: Long) extends AbstractLongSearchScript {

  override def runAsLong: Long = {
    val fieldValue = doc.get(field).asInstanceOf[ScriptDocValues.Doubles].getValues.map(_.toFloat).filter(mapper.isDefinedAt)
    if(fieldValue.nonEmpty)
      mapper apply fieldValue.head
    else missing
  }
}

class DoubleFieldOrdinalizerScript(field: String, mapper: PartialFunction[Double, Long], missing: Long) extends AbstractLongSearchScript {

  override def runAsLong: Long = {
    val fieldValue = doc.get(field).asInstanceOf[ScriptDocValues.Doubles].getValues.filter(mapper isDefinedAt _)
    if(fieldValue.nonEmpty)
      mapper apply fieldValue.head
    else missing
  }
}

class StringFieldOrdinalizerScript(field: String, mapper: PartialFunction[String, Long], missing: Long) extends AbstractLongSearchScript {

  override def runAsLong: Long = {
    val fieldValue = doc.get(field).asInstanceOf[ScriptDocValues.Strings].getValues.filter(mapper.isDefinedAt)
    if(fieldValue.nonEmpty)
      mapper apply fieldValue.head
    else missing
  }
}