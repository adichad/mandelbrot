package com.askme.mandelbrot.handler

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import spray.json.{JsArray, JsValue, JsonParser}

/**
 * Created by adichad on 19/03/15.
 */
class IndexRequestHandler(val config: Config, serverContext: SearchContext) extends Actor with Logging with Configurable {

  //private val producer = serverContext.kafkaProducer

  override def receive = {
    case indexParams: IndexingParams =>
      val json = JsonParser(indexParams.data.data).asInstanceOf[JsArray]
      for(doc: JsValue <- json.elements) {
        info(doc)
        /*producer.send(
          new ProducerRecord(
            indexParams.idx.esType,
            doc.extract[String]('PlaceID.?).get.getBytes(Charset.forName("UTF-8")),
            doc.toString.getBytes(Charset.forName("UTF-8"))
          )
        )*/
      }


      context.parent ! IndexResult(true)
  }

}
