package com.askme.mandelbrot.loader

import java.io.File
import java.nio.file.Path

import akka.actor.{ActorRef, Props, Actor}
import akka.event.LoggingReceive
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging

/**
 * Created by adichad on 14/11/14.
 */

sealed trait FileSystemChange
case class Created(fileOrDir: File, origPath: Path) extends FileSystemChange
case class Deleted(fileOrDir: File, origPath: Path) extends FileSystemChange
case class Modified(fileOrDir: File, origPath: Path) extends FileSystemChange

case class MonitorDir(path: Path, index: String, esType: String)


class FileSystemWatcher(val config: Config, serverContext: SearchContext) extends Actor with Configurable with Logging {
  val watchServiceTask = new WatchServiceTask(self)
  val watchThread = new Thread(watchServiceTask, "WatchService")
  val registry = new java.util.HashMap[Path, ActorRef]

  override def preStart() {
    watchThread.setDaemon(true)
    watchThread.start()
    info("started watch thread")
  }

  override def postStop() {
    watchThread.interrupt()
  }

  def receive = LoggingReceive {
    case MonitorDir(path, index, esType) =>
      watchServiceTask watchRecursively path
      register(path, index, esType)
    case Created(file, origPath) =>
      registry.get(origPath) ! Index(file)
    case Modified(file, origPath) =>
      registry.get(origPath) ! Index(file)
    case Deleted(file, origPath) =>

  }

  private def register(path: Path, index: String, esType: String) = {
    if(!registry.containsKey(path)) {
      val name = "loader-" + path.toString.replace("/", "=")
      registry.put(path, context.actorOf(Props(classOf[CSVLoader],
        config, index, esType, serverContext.batchExecutionContext, serverContext), name))
      info("watching path ["+path+"] for index ["+index+"], type ["+esType+"]")
    }
  }
}
