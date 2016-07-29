package com.askme.mandelbrot.util
import com.netflix.config.{DynamicConfiguration, DynamicMapProperty, DynamicPropertyFactory, FixedDelayPollingScheduler}
import com.typesafe.config.Config
/**
  * Created by Nihal on 25/07/16.
  */
object GlobalDynamicConfiguration {
  var dynamicProps:DynamicPropertyFactory=null
  var polledConfig:Config = null
  def setDynamicProps(dp:DynamicPropertyFactory){
    dynamicProps = dp
  }

  def getDynamicProps():DynamicPropertyFactory={
    dynamicProps
  }

  def getDynamicConfig(config: Config):DynamicPropertyFactory = {

    val scheduler = new FixedDelayPollingScheduler(0, 2000, false)
    val cw = new ConfigWrapper(config)
    val configuration = new DynamicConfiguration(cw, scheduler)
    DynamicPropertyFactory.initWithConfigurationSource(configuration)
    return DynamicPropertyFactory.getInstance()
  }

}
