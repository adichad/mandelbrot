package com.askme.mandelbrot.scripts

import java.util

import grizzled.slf4j.Logging
import org.elasticsearch.index.fielddata.ScriptDocValues.Strings
import org.elasticsearch.script.{AbstractLongSearchScript, ExecutableScript, NativeScriptFactory}

import scala.collection.JavaConversions._
/**
 * Created by adichad on 12/05/15.
 */
class CustomerTypeBucket extends NativeScriptFactory with Logging {

  override def newScript(params: util.Map[String, AnyRef]): ExecutableScript = {
    /*info("new script")
    SearchContext.current().innerHits().getInnerHits.values.map(h => h.parsedQuery().namedFilters().keySet()).foreach { (filters: ImmutableSet[String]) =>
      info("_____")
      filters.foreach { info(_) }
    }*/
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
