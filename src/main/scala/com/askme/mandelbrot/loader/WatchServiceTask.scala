package com.askme.mandelbrot.loader

import java.nio.file.StandardWatchEventKinds._
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import akka.actor.ActorRef
import grizzled.slf4j.Logging

import scala.collection.JavaConversions._

/**
 * Created by adichad on 14/11/14.
 */

class WatchServiceTask(notifyActor: ActorRef) extends Runnable with Logging {
  private val watchService = FileSystems.getDefault.newWatchService()

  def watchRecursively(root: Path) {
    watch(root)
    Files.walkFileTree(root, new SimpleFileVisitor[Path] {
      override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes) = {
        watch(dir)
        FileVisitResult.CONTINUE
      }
    })
  }

  private def watch(path: Path) = {
    path.register(watchService, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY)

  }

  def run() {
    try {
      while (!Thread.currentThread.isInterrupted) {
        val key = watchService.take()
        key.pollEvents() foreach {
          event =>
            val relativePath = event.context().asInstanceOf[Path]
            val origPath = key.watchable().asInstanceOf[Path]
            val path = origPath.resolve(relativePath)

            event.kind() match {
              case ENTRY_CREATE =>
                if (path.toFile.isDirectory) {
                  watchRecursively(path)
                }
                notifyActor ! Created(path.toFile, origPath)
              case ENTRY_DELETE =>
                notifyActor ! Deleted(path.toFile, origPath)
              case ENTRY_MODIFY =>
                notifyActor ! Modified(path.toFile, origPath)
              case x =>
                warn(s"Unknown event $x")
            }
        }
        key.reset()
      }
    } catch {
      case e: InterruptedException =>
        logger.info("Interrupted, bye!")
    } finally {
      watchService.close()
    }
  }


}
