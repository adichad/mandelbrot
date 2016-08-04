package com.askme.mandelbrot.util
import com.netflix.config.{DynamicConfiguration, DynamicPropertyFactory, FixedDelayPollingScheduler}
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry

/**
  * Created by Nihal on 25/07/16.
  */
object GlobalDynamicConfiguration extends Logging{
  var dynamicProps:DynamicPropertyFactory=null
  var polledConfig:Config = null
  var zkClient:CuratorFramework = null
  def setDynamicProps(dp:DynamicPropertyFactory){
    dynamicProps = dp
  }

  def getDynamicProps():DynamicPropertyFactory={
    dynamicProps
  }

  def startZookeeper(config: Config) = {
    try {
      val connectionString = config getString "zkConString"
      zkClient = CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(1000, 3))
      zkClient.start()
    } catch {
      case ae: Exception => {
        warn(ae.getMessage)
      }
    }
    if(zkClient != null)
      info("Connection to Zookeeper successful!")
    else
      warn("Connection to Zookeeper failed! Will retry in next iteration")
  }

  def stopZookeeper(): Unit ={
    zkClient.close()
  }

  def getDynamicConfig(config: Config):DynamicPropertyFactory = {
    startZookeeper(config)
    val scheduler = new FixedDelayPollingScheduler(0, config getInt "pollerInterval", false)
    val cw = new ConfigWrapper(config)
    val configuration = new DynamicConfiguration(cw, scheduler)
    DynamicPropertyFactory.initWithConfigurationSource(configuration)
    return DynamicPropertyFactory.getInstance()
  }

}
