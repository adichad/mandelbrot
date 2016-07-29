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
      val polledConfig = ConfigFactory.load(ConfigFactory.parseFile( new File("/Users/Nihal/search/mandelbrot/src/main/resources/environment_defaults.conf")).withFallback(configuration))
      GlobalDynamicConfiguration.polledConfig = polledConfig
      polledConfig
//    try {
//      val connectionString = "127.0.0.1:2181"
//      val CONFIG_ROOT_PATH = "/test"
//      client = CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(1000, 3))
//      client.start()
//      confString = new String(client.getData.forPath("/test"))
//    } catch {
//      case ae:Exception=>{
//        error(ae.getMessage)
//      }
//    } finally {
//      configrtn =ConfigFactory.load(ConfigFactory.parseString(confString).withFallback(configuration))
//      this.synchronized {
//        GlobalDynamicConfiguration.polledConfig = configrtn
//      }
//      client.close()
//    }
//    return configrtn
  }
}


