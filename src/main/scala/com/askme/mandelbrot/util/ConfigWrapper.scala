package com.askme.mandelbrot.util

import com.netflix.config.sources.TypesafeConfigurationSource
import com.typesafe.config._
import grizzled.slf4j.Logging



/**
  * Created by Nihal on 19/07/16.
  */
object ConfigWrapper extends TypesafeConfigurationSource with Logging {

  var configuration: Config = _
  def init(configuration:Config): Unit = {
    this.configuration = configuration
  }

  override protected def config(): Config = {
    val confString =
      if (GlobalDynamicConfiguration.zkClient != null)
        new String(GlobalDynamicConfiguration.zkClient.getData.forPath(configuration getString "zkRootPath"))
      else {
        ""
      }

    if (confString.isEmpty) {
      warn("No configuration received from zookeeper, Re-trying to connect to Zookeeper")
      GlobalDynamicConfiguration.startPolling()
    }
    debug("dynamic configuration: "+confString)

    synchronized {
      GlobalDynamicConfiguration.config = ConfigFactory.load(ConfigFactory.parseString(confString).withFallback(configuration))
    }
    GlobalDynamicConfiguration.config
  }
}


