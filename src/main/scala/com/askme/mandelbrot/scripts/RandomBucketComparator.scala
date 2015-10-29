package com.askme.mandelbrot.scripts

import java.util

import org.elasticsearch.script.{AbstractLongSearchScript, ExecutableScript, NativeScriptFactory}
import scala.collection.JavaConversions._
import scala.util.Random

/**
 * Created by adichad on 05/10/15.
 */
class RandomBucketComparator extends NativeScriptFactory {
  override def newScript(params: util.Map[String, AnyRef]): ExecutableScript = {
    new RandomBucketComparatorScript(
      params.get("buckets").asInstanceOf[Int])
  }
}

object RandomBucketComparatorScript {
  val randomizer = new Random()
}
class RandomBucketComparatorScript(buckets: Int) extends AbstractLongSearchScript {
  import RandomBucketComparatorScript._
  override def runAsLong: Long = {
    randomizer.nextInt(buckets)
  }
}

