package com.askme.mandelbrot.stream

import scala.collection.mutable.LinkedList
import scala.reflect.ClassTag
import scala.util.Random
import scala.xml.XML
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.receiver.ActorHelper
import org.apache.spark.streaming.receiver.Receiver
import com.askme.mandelbrot.server.Server
import com.typesafe.config.Config
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSelection.toScala
import akka.actor.Props
import akka.actor.SupervisorStrategy

import grizzled.slf4j.Logging
import com.askme.mandelbrot.Configurable

case class SubscribeReceiver(receiverActor: ActorRef)
case class UnsubscribeReceiver(receiverActor: ActorRef)


case class ReceiveTask(val v: String)

class DuilwenStream(val config: Config) extends Logging with Configurable {
  private val sc = new SparkContext((new SparkConf).setMaster(string("master")).setAppName(string("name")))
  private val ssc: StreamingContext = new StreamingContext(sc, Seconds(int("batch-duration")))

  (1 to int("feeder.count")) map { _ ⇒ ssc.actorStream(Props(Class.forName(string("feeder.type"))), "Feeder", StorageLevel.MEMORY_ONLY_2, SupervisorStrategy.defaultStrategy) }

  val multiStream = ssc.union((1 to int("worker.count")) map { _ ⇒
    ssc.actorStream[String](Props(Class.forName(string("worker.type"))), "Worker", StorageLevel.MEMORY_ONLY_2, SupervisorStrategy.defaultStrategy)
  })
  multiStream.map(msg ⇒ (for (bean ← (XML loadString msg) \\ "MBean") yield (bean \ "MBeanName")).mkString(" <--> ") //+ ":" + (bean \\ "MBeanAttributeList" \\ "Name" \ "Value" \ "SimpleValue" \ "Value").head.text 
  //+ ":" + (for(x <- bean \\ "MBeanAttributeList" \\ "Name") yield if(x.text=="HeapMemoryUsage") (x \ "Value" \ "SimpleValue" \ "Value").text else "")
  ).print

  ssc.start
  //ssc.awaitTermination
}
