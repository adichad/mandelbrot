package com.askme.mandelbrot

import java.io.{Closeable, File, PrintWriter}
import java.lang.management.ManagementFactory

import com.askme.mandelbrot.server.Server
import com.hazelcast.core.Hazelcast
import com.typesafe.config.{ConfigFactory, Config}
import grizzled.slf4j.Logging

object Launcher extends App with Logging with Configurable {

  protected[this] val config = configure("environment", "application", "environment_defaults", "application_defaults")
  private val hazel = Hazelcast.newHazelcastInstance
  try {
    // hack to make configuration parameters available in logback.xml
    backFillSystemProperties("component.name", "log.path.current", "log.path.archive", "log.level")
    info(string("component.name"))
    info("Log path: " + string("log.path.current"))

    writePID(string("daemon.pidfile"))
    if (boolean("sysout.detach")) System.out.close()
    if (boolean("syserr.detach")) System.err.close()
    val servers = map[Server]("server").values
    closeOnExit(servers)
    servers.foreach(_.bind)
  } catch {
    case e: Throwable =>
      error("fatal", e)
      throw e
  }

  private[this] def configure(resourceBases: String*) =
    ConfigFactory.load((for (base ← resourceBases) yield ConfigFactory.parseResourcesAnySyntax(base)).reduceLeft(_ withFallback _).withFallback(ConfigFactory.systemEnvironment()))
  
  private[this] def backFillSystemProperties(propertyNames: String*) =
    for (propertyName ← propertyNames) System.setProperty(propertyName, string(propertyName))

  private[this] def writePID(destPath: String) = {
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
      (new PrintWriter(pidFile) append pid("<Unknown-PID>")).close()
      pidFile.deleteOnExit()
      info("pid file: " + destPath)
      true
    } else {
      error("unable to write pid file, exiting.")
      System exit 1
      false
    }
  }

  private[this] def closeOnExit(closeables: Iterable[Closeable]) = {
    Runtime.getRuntime addShutdownHook new Thread {
      override def run() = {
        try {
          closeables.foreach(_.close)
        } catch {
          case e: Throwable => error("shutdown hook failure", e)
        }
      }
    }
  }

}
