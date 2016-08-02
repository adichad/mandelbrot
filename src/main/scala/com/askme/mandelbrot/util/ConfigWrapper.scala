package com.askme.mandelbrot.util

import com.netflix.config.sources.TypesafeConfigurationSource
import com.typesafe.config._
import grizzled.slf4j.Logging



/**
  * Created by Nihal on 19/07/16.
  */
class ConfigWrapper(val configuration:Config) extends TypesafeConfigurationSource with Logging {
  protected[this] var configrtn: Config = null
  protected[this] var confString = ""

  override protected def config(): Config = {
    val CONFIG_ROOT_PATH = configuration getString "zkRootPath"

    if (GlobalDynamicConfiguration.zkClient != null)
      confString = new String(GlobalDynamicConfiguration.zkClient.getData.forPath(CONFIG_ROOT_PATH))
    else {
      confString = "{}"
      info("No configuration received from zookeeper, Re-trying to connect to Zookeeper")
      GlobalDynamicConfiguration.startZookeeper(configuration)
    }
    configrtn = ConfigFactory.load(ConfigFactory.parseString(confString).withFallback(configuration))
    this.synchronized {
      GlobalDynamicConfiguration.polledConfig = configrtn
    }
    return configrtn
  }
}


