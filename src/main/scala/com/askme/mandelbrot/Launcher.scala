package com.askme.mandelbrot

import java.io.Closeable
import java.io.File
import java.io.PrintWriter
import java.lang.management.ManagementFactory

import scala.collection.JavaConversions._

import com.askme.mandelbrot.server.Server
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import grizzled.slf4j.Logging

object Launcher extends App with Logging with Configurable {

  protected[this] val config = configure("environment", "application", "environment_defaults", "application_defaults")

  try {
    // hack to make configuration parameters available in logback.xml
    backFillSystemProperties("component.name", "log.path.current", "log.path.archive", "log.level")
    info(string("component.name"))
    info("Log path: " + string("log.path.current"))
    daemonize(map[Server]("server").values)
  } catch {
    case e: Throwable => {
      error("fatal", e)
      throw e
    }
  }

  private[this] def configure(resourceBases: String*) =
    ConfigFactory.load((for (base ← resourceBases) yield ConfigFactory.parseResourcesAnySyntax(base)).reduceLeft(_ withFallback _))
  
  private[this] def backFillSystemProperties(propertyNames: String*) =
    for (propertyName ← propertyNames) System.setProperty(propertyName, string(propertyName))
  

  private[this] def daemonize(servers: Iterable[Server]) = {
    def writePID(destPath: String) = {
      def pid(fallback: String) = {
        val jvmName = ManagementFactory.getRuntimeMXBean.getName
        val index = jvmName indexOf '@'
        if (index > 0) {
          try {
            jvmName.substring(0, index).toLong.toString
          } catch {
            case e: NumberFormatException ⇒ fallback
          }
        } else fallback
      }

      val pidFile = new File(destPath)
      if (pidFile.createNewFile) {
        (new PrintWriter(pidFile) append pid("<Unknown-PID>")).close
        pidFile.deleteOnExit
        info("pid file: " + destPath)
        true
      } else {
        error("unable to write pid file, exiting.")
        System exit 1
        false
      }
    }

    def closeOnExit(closeables: Iterable[Closeable]) = {
      Runtime.getRuntime addShutdownHook new Thread {
        override def run = {
          try {
            closeables.foreach(_.close)
          } catch {
            case e: Throwable => error("shutdown hook failure", e)
          }
        }
      }
    }
    closeOnExit(servers)
    writePID(string("daemon.pidfile"))
    if (boolean("sysout.detach")) System.out.close
    if (boolean("syserr.detach")) System.err.close
    servers.foreach(_.bind)

  }

}
