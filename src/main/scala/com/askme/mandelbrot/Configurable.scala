package com.askme.mandelbrot

import java.util.{List, Map, Properties}

import com.askme.mandelbrot.piper.Piper
import com.typesafe.config.{Config, ConfigFactory}
import grizzled.slf4j.Logging
import org.elasticsearch.common.settings.Settings

import scala.collection.JavaConversions.{asScalaBuffer, mapAsScalaMap}
import scala.reflect.ClassTag
import com.askme.mandelbrot.util.GlobalDynamicConfiguration

import scala.collection.JavaConversions._


trait Configurable extends Logging {
  protected[this] val parentPath: String

  protected[this] def conf(part: String) = {
    val index = getIndex(part)._1
    val partMod = getIndex(part)._2
    if (index == Int.MinValue)
      GlobalDynamicConfiguration.polledConfig.getConfig(parentPath(part))
    else {
      GlobalDynamicConfiguration.polledConfig.getConfigList(parentPath(partMod))(index)
    }
  }

  protected[this] def parentPath(part : String = ""):String = if (part == "") parentPath else (if (parentPath == "") part else (parentPath+"."+part) )

  protected[this] def confs(part: String) = {
    val index = getIndex(part)._1
    val partMod = getIndex(part)._2
    if (index == Int.MinValue)
      GlobalDynamicConfiguration.polledConfig.getConfigList(parentPath(part))
    else{
      GlobalDynamicConfiguration.polledConfig.getList(parentPath(partMod))(index).asInstanceOf[java.util.List[_ <: Config]]
    }
  }


  protected[this] def map[T](part: String)(implicit t: ClassTag[T])= {
    if (classOf[Configurable] isAssignableFrom t.runtimeClass)
      GlobalDynamicConfiguration.polledConfig.getAnyRef(parentPath(part)).asInstanceOf[Map[String, Map[String, AnyRef]]].map(x ⇒ (x._1, obj( if (part == "") "" else part+ "."  + x._1).asInstanceOf[T]))
    else
      mapAsScalaMap(GlobalDynamicConfiguration.polledConfig.getAnyRef(parentPath(part)).asInstanceOf[Map[String, T]])
  }

  protected[this] def list[T](part: String)(implicit t: ClassTag[T]):scala.collection.immutable.List[T] = {
    if (classOf[Configurable] isAssignableFrom t.runtimeClass){
      val confList : java.util.List[_<:Config] = confs(parentPath(part))
      val objList:java.util.List[T] = new java.util.ArrayList[T]()
      var i = 0
      for (i <- 0 to confList.size()){
        objList.add(obj( parentPath(part) + "[" + i + "]"))
      }
      objList.toList
    }
    else {
      val index = getIndex(part)._1
      val partMod = getIndex(part)._2
      val variable = part.substring(part.indexOf("]")+2)
      if (index == Int.MinValue) {
        GlobalDynamicConfiguration.polledConfig.getAnyRef(parentPath(part)).asInstanceOf[List[T]].toList
      }
      else{
        GlobalDynamicConfiguration.polledConfig.getConfigList(parentPath(partMod))(index).getStringList(variable).asInstanceOf[List[T]].toList
      }
    }
  }


  //Start Here
  protected[this] def obj[T <: Configurable](part: String) = {
    val index = getIndex(part)._1
    val partMod = getIndex(part)._2
    if (index == Int.MinValue) {
      Class.forName(GlobalDynamicConfiguration.polledConfig.getString(parentPath(part) + ".type")).getConstructor(classOf[String]).newInstance(parentPath(part)).asInstanceOf[T]
    }
    else{
      Class.forName(GlobalDynamicConfiguration.polledConfig.getConfigList(parentPath(partMod))(index).getString("type")).getConstructor(classOf[String]).newInstance(parentPath(partMod)+"[" +index.toString +"]").asInstanceOf[T]
    }
  }
  protected[this] def objs[T <: Configurable](part: String): scala.collection.immutable.List[T] = {
    val confList : java.util.List[_<:Config] = confs(parentPath(part))
    val objList:java.util.List[T] = new java.util.ArrayList[T]()
    var i = 0
    for (i <- 0 to confList.size()){
      objList.add(obj(parentPath(part) + "[" + i + "]"))
    }
    objList.toList
  }

  protected[this] def piper(part: String): Piper = obj[Piper](part)

  protected[this] def pipers(part: String): Seq[Piper] = objs[Piper](part)

  protected[this] def keys(part: String):java.util.Set[_ <: Any]={
    val index = getIndex(part)._1
    val partMod = getIndex(part)._2
    if (index == Int.MinValue) {
      GlobalDynamicConfiguration.polledConfig.getAnyRef(parentPath(part)).asInstanceOf[Map[String, Any]].keySet
    }
    else{
      GlobalDynamicConfiguration.polledConfig.getAnyRefList(parentPath(partMod))(index).asInstanceOf[Map[String, Any]].keySet
    }
  }
  protected[this] def vals[T](part: String) :java.util.Collection[_ <: Any]= {
    val index = getIndex(part)._1
    val partMod = getIndex(part)._2
    if (index == Int.MinValue) {
      GlobalDynamicConfiguration.polledConfig.getAnyRef(parentPath(part)).asInstanceOf[Map[String, Any]].values
    }
    else{
      GlobalDynamicConfiguration.polledConfig.getAnyRefList(parentPath(partMod))(index).asInstanceOf[Map[String, Any]].values
    }
  }

  protected[this] def bytes(part: String) = {
    val index = getIndex(part)._1
    val partMod = getIndex(part)._2
    val variable = part.substring(part.indexOf("]")+2)
    if (index == Int.MinValue) {
      GlobalDynamicConfiguration.polledConfig.getBytes(parentPath(part))
    }
    else{
      GlobalDynamicConfiguration.polledConfig.getConfigList(parentPath(partMod))(index).getBytes(variable)
    }
  }
  protected[this] def boolean(part: String): Boolean = {
    val index = getIndex(part)._1
    val partMod = getIndex(part)._2
    val variable = part.substring(part.indexOf("]")+2)
    if (index == Int.MinValue) {
      GlobalDynamicConfiguration.polledConfig.getBoolean(parentPath(part))
    }
    else{
      GlobalDynamicConfiguration.polledConfig.getConfigList(parentPath(partMod))(index).getBoolean(variable)
    }
  }
  protected[this] def int(part: String) = {
    val index = getIndex(part)._1
    val partMod = getIndex(part)._2
    val variable = part.substring(part.indexOf("]") + 2)
    if (index == Int.MinValue) {
      GlobalDynamicConfiguration.polledConfig.getInt(parentPath(part))
    }
    else {
      GlobalDynamicConfiguration.polledConfig.getConfigList(parentPath(partMod))(index).getInt(variable)
    }
  }
  protected[this] def long(part: String) = {
    val index = getIndex(part)._1
    val partMod = getIndex(part)._2
    val variable = part.substring(part.indexOf("]") + 2)
    if (index == Int.MinValue) {
      GlobalDynamicConfiguration.polledConfig.getLong(parentPath(part))
    }
    else {
      GlobalDynamicConfiguration.polledConfig.getConfigList(parentPath(partMod))(index).getLong(variable)
    }
  }
  protected[this] def double(part: String) = {
    val index = getIndex(part)._1
    val partMod = getIndex(part)._2
    val variable = part.substring(part.indexOf("]") + 2)
    if (index == Int.MinValue) {
      GlobalDynamicConfiguration.polledConfig.getDouble(parentPath(part))
    }
    else {
      GlobalDynamicConfiguration.polledConfig.getConfigList(parentPath(partMod))(index).getDouble(variable)
    }
  }
  protected[this] def string(part: String) = {
    val index = getIndex(part)._1
    val partMod = getIndex(part)._2
    val variable = part.substring(part.indexOf("]") + 2)
    if (index == Int.MinValue) {
      GlobalDynamicConfiguration.polledConfig.getString(parentPath(part))
    }
    else {
      GlobalDynamicConfiguration.polledConfig.getConfigList(parentPath(partMod))(index).getString(variable)
    }
  }//config getString part

  protected[this] def configure(resourceBases: String*): Config = {
    ConfigFactory.load((for (base ← resourceBases) yield ConfigFactory.parseResourcesAnySyntax(base)).reduceLeft(_ withFallback _).withFallback(ConfigFactory.systemEnvironment()))
  }
  protected[this] def backFillSystemProperties(propertyNames: String*) =
    for (propertyName ← propertyNames) System.setProperty(propertyName, string(propertyName))

  protected[this] def props(conf: Config) = {
    val p = new Properties
    for( e <- conf.entrySet())
      p.setProperty(e.getKey, conf.getString(e.getKey))
    p
  }

  protected[this] def props(part: String): Properties = props(conf(parentPath(part)))


  protected[this] def settings(part: String) = {
    val settings = Settings.settingsBuilder()
    val c = conf(part)
    for( e <- c.entrySet() ) {
      try {
        settings.put(e.getKey, c.getString(e.getKey))
      } catch {
        case ex:Exception => settings.putArray(e.getKey, c.getStringList(e.getKey):_*)
      }
    }
    settings.build()
  }

  protected[this] def getIndex(PropKey:String) ={
    val key = PropKey.trim
    if(!key.contains("]"))
      (Int.MinValue,"")
    else {
      val partMod = key.substring(0, key.indexOf("["))
      (key.substring(key.indexOf("[") + 1, key.indexOf("]")).toInt, partMod)
    }
  }
}
