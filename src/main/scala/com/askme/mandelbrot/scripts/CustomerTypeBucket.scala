package com.askme.mandelbrot.scripts

import java.util

import org.elasticsearch.index.fielddata.ScriptDocValues.Strings
import org.elasticsearch.script.{AbstractLongSearchScript, ExecutableScript, NativeScriptFactory}

import scala.collection.JavaConversions._
/**
 * Created by adichad on 12/05/15.
 */
class CustomerTypeBucket extends NativeScriptFactory {


  override def newScript(params: util.Map[String, AnyRef]): ExecutableScript = {
    new CustomerTypeBucketScript
  }
}

class CustomerTypeBucketScript extends AbstractLongSearchScript {
  override def runAsLong: Long = {
    if (doc.get("CustomerType").asInstanceOf[Strings].getValues.filter(_.equals("350")).size>0)
      0
    else
      1
  }
}
