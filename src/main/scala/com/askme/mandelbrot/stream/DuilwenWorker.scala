package com.askme.mandelbrot.stream

import akka.actor.Actor
import dispatch.Defaults.executor
import dispatch.{Http, as, enrichFuture, host, implyRequestHandlerTuple}
import org.apache.spark.streaming.receiver.ActorHelper

import scala.concurrent.{blocking, future}


class DuilwenWorker extends Actor with ActorHelper {

  override def preStart = {
    val actors = context.system.actorSelection("akka://spark/user/Supervisor*/Feeder*")
    actors ! SubscribeReceiver(context.self)
    System.out.println("Querier preStart: " + actors)
  }

  override def receive = {
    case task: ReceiveTask ⇒ {

      val x = host("chelcarjvasc02:9090") / "MonitoringWS" / "v1" / "jmx" / "localhost" / "52097" / "CarMicronNexusSCS" / "mbeanData" / "java.lang"
      val jmx = Http(x OK as.String)
      System.out.println("scheduling query for bean")
      future {
        blocking {
          System.out.println("firing query for bean")
          store(jmx())
        }
      }
      System.out.println("Done scheduling query for bean")
    }

  }

  override def postStop = context.system.actorSelection("akka://spark/user/Supervisor*/Feeder*") ! UnsubscribeReceiver(context.self)

}
