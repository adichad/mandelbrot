package com.askme.mandelbrot.stream

import org.apache.spark.streaming.receiver.ActorHelper
import scala.util.Random
import scala.collection.mutable.LinkedList
import akka.actor.Actor
import akka.actor.ActorRef

class DuilwenFeeder extends Actor with ActorHelper {
  val rand = new Random()
  println(context.self)
  var receivers: LinkedList[ActorRef] = new LinkedList[ActorRef]()

  val strings: Array[String] = Array("words ", "may ", "count ")

  def makeMessage(): ReceiveTask = {
    //zookeeper logic
    ReceiveTask(strings(rand.nextInt(3)))
  }

  /*
   * A thread to generate random messages
   */
  new Thread() {
    override def run() {
      while (true) {
        Thread sleep 100
        if (receivers.size > 0)
          receivers.get(rand nextInt receivers.size).get ! makeMessage
      }
    }
  }.start

  override def receive: Receive = {
    case SubscribeReceiver(receiverActor: ActorRef) ⇒
      System.out.println("%s: received subscribe from %s".format(context.self, receiverActor.toString))
      receivers = LinkedList(receiverActor) ++ receivers

    case UnsubscribeReceiver(receiverActor: ActorRef) ⇒
      System.out.println("%s: received unsubscribe from %s".format(context.self, receiverActor.toString))
      receivers = receivers.dropWhile(x ⇒ x eq receiverActor)
  }
}