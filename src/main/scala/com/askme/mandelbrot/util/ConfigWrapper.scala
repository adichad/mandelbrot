package com.askme.mandelbrot.util

import java.io.File

import com.netflix.config.sources.TypesafeConfigurationSource
import com.typesafe.config._
import grizzled.slf4j.Logging
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.retry.ExponentialBackoffRetry



/**
  * Created by Nihal on 19/07/16.
  */
class ConfigWrapper(val configuration:Config) extends TypesafeConfigurationSource with Logging{
  protected[this] var client:CuratorFramework = null
  protected [this] var configrtn:Config = null
  protected[this] var confString = ""
  override protected def config():Config= {
    try {
      val connectionString = configuration getString "zkConString"
      val CONFIG_ROOT_PATH = configuration getString "zkRootPath"
      client = CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(1000, 3))
      client.start()
      confString = new String(client.getData.forPath(CONFIG_ROOT_PATH))
    } catch {
      case ae:Exception=>{
        error(ae.getMessage)
      }
    } finally {
      configrtn =ConfigFactory.load(ConfigFactory.parseString(confString).withFallback(configuration))
      this.synchronized {
        GlobalDynamicConfiguration.polledConfig = configrtn
      }
      client.close()
    }
    return configrtn
  }
}


