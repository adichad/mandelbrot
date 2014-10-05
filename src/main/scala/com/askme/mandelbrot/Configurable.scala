package com.askme.mandelbrot

import java.util.{List, Map}

import scala.collection.JavaConversions.{asScalaBuffer, mapAsScalaMap}
import scala.reflect.ClassTag

import com.typesafe.config.{Config, ConfigFactory}

trait Configurable {
  protected[this] val config: Config

  protected[this] def conf(part: String) = config getConfig part
  protected[this] def confs(part: String) = config getConfigList part
  protected[this] def map[T](part: String)(implicit t: ClassTag[T]) = {
    if (classOf[Config] isAssignableFrom t.runtimeClass)
      (config getAnyRef part).asInstanceOf[Map[String, Map[String, AnyRef]]].map(x ⇒ (x._1, (ConfigFactory parseMap x._2).asInstanceOf[T]))
    else if (classOf[Configurable] isAssignableFrom t.runtimeClass)
      (config getAnyRef part).asInstanceOf[Map[String, Map[String, AnyRef]]].map(x ⇒ (x._1, obj(ConfigFactory parseMap x._2).asInstanceOf[T]))
    else
      mapAsScalaMap((config getAnyRef part).asInstanceOf[Map[String, T]])
  }

  protected[this] def list[T](part: String)(implicit t: ClassTag[T]) = {
    if (classOf[Config] isAssignableFrom t.runtimeClass)
      (config getConfigList part).asInstanceOf[java.util.List[T]].toList
    else if (classOf[Configurable] isAssignableFrom t.runtimeClass)
      (config getConfigList part).asInstanceOf[List[Config]].map(obj(_).asInstanceOf[T]).toList
    else
      (config getAnyRef part).asInstanceOf[List[T]].toList
  }

  protected[this] def obj[T <: Configurable](conf: Config) = Class.forName(conf getString "type").getConstructor(classOf[Config]).newInstance(conf).asInstanceOf[T]
  protected[this] def obj[T <: Configurable](part: String): T = obj[T](conf(part))

  protected[this] def keys(part: String) = (config getAnyRef part).asInstanceOf[Map[String, Any]].keySet
  protected[this] def vals[T](part: String) = (config getAnyRef part).asInstanceOf[Map[String, T]].values

  protected[this] def bytes(part: String) = config getBytes part
  protected[this] def boolean(part: String) = config getBoolean part
  protected[this] def int(part: String) = config getInt part
  protected[this] def long(part: String) = config getLong part
  protected[this] def double(part: String) = config getDouble part
  protected[this] def string(part: String) = config getString part
}
