package com.askme.mandelbrot.piper

import java.security.MessageDigest

import akka.actor.ActorRef
import com.askme.mandelbrot.handler.{IndexFailureResult, IndexSuccessResult}
import com.askme.mandelbrot.server.RootServer
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.analyze.{AnalyzeAction, AnalyzeRequestBuilder}
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.client.Client
import org.json4s.JsonAST.JValue
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.JavaConversions._

/**
 * Created by adichad on 04/01/16.
 */
class ProductSuggestPiper(val config: Config) extends Piper with Logging {
  //val producer = RootServer.defaultContext.kafkaProducer
  private def md5(s: String) =
    MessageDigest.getInstance("MD5").digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString

  val esClient = RootServer.defaultContext.esClient

  private def analyze(esClient: Client, index: String, field: String, text: String): Array[String] =
    new AnalyzeRequestBuilder(esClient.admin.indices, AnalyzeAction.INSTANCE, index, text).setField(field).get().getTokens.map(_.getTerm).toArray

  implicit class ProductJValue(doc: JValue) {
    def categories =
      (doc \ "categories").children.map(c =>
        (c \ "name").asInstanceOf[JString].values.trim).filter(!_.isEmpty)

    def city = {
      (doc \ "subscriptions").children.map(s=>s\"ndd_city").map(
        c=>if(c==null||c==JNull)"all city" else c.asInstanceOf[JString].values.trim)

    }

    def label = (doc \ "name").asInstanceOf[JString].values.trim

    def deleted = 1 - (if (doc.is_deleted) 0 else (doc \ "status").asInstanceOf[JInt].values.toInt)

    def base_product_id = (doc \ "base_product_id").asInstanceOf[JInt].values.toString()

    def product_id = (doc \ "product_id").asInstanceOf[JInt].values.toString()

    def kw =
      ((doc \ "categories").children.map(c=>(c\"name").asInstanceOf[JString].values.trim).filter(!_.isEmpty) ++
        (doc \ "categories").children.map(c=>(c\"description").asInstanceOf[JString].values.trim).filter(!_.isEmpty)) :+
        (doc \ "name").asInstanceOf[JString].values.trim

    def orders = 0

    def stores = (doc\"stores").children.map(s=>(s\"name").asInstanceOf[JString].values)

    def store_fronts = (doc\"subscriptions").children.flatMap(s=>(s\"store_fronts").children.map(sf=>"store_front_"+(sf\"id").asInstanceOf[JInt].values))

    def image = {
      val img = doc\"image"
      if(img==null||img==JNull)"" else img.asInstanceOf[JString].values
    }

    def is_deleted = (doc\"is_deleted").asInstanceOf[JBool].value

  }


  override def pipe(json: JValue, completer: ActorRef): Unit = {
    val startTime = System.currentTimeMillis()
    try {
      val bulkRequest = RootServer.defaultContext.esClient.prepareBulk
      for(doc <- json.children) {
        val suggestion: JValue = if(doc.is_deleted)
          ("id"->doc.base_product_id) ~
            ("deleted"->1) ~
            ("groupby" -> doc.base_product_id) ~
            ("targeting" -> List())
        else
          ("id" -> doc.base_product_id) ~
          ("targeting" ->
            List(
              ("city" -> doc.city) ~
              ("kw" -> doc.kw) ~
              ("label" -> doc.label) ~
              ("tag" -> (List("unified", "base_product") ++ doc.stores ++ doc.store_fronts))
            )
          ) ~
          ("payload" ->
            List(
              ("queries" ->
                List(
                  ("type" -> "base_product") ~
                  ("id" -> doc.base_product_id)
                )
              ) ~
              ("display" ->
                ("label" -> doc.label) ~
                ("categories" -> doc.categories) ~
                ("type" -> "base_product") ~
                ("stores" -> doc.stores) ~
                ("image" -> doc.image)
              )
            )
          ) ~
          ("deleted" -> doc.deleted) ~
          ("groupby" -> doc.base_product_id) ~
          ("count" -> doc.orders)


          bulkRequest.add(
            esClient.prepareIndex(string("params.index"), string("params.type"), doc.product_id.values+"-base_product")
              .setSource(compact(render(suggestion)))
          )

        }
        val reqSize = bulkRequest.numberOfActions()
        bulkRequest.execute(new ActionListener[BulkResponse] {
          override def onResponse(response: BulkResponse): Unit = {
            try {
              val failures = "[" + response.getItems.filter(_.isFailed).map(x => "{\""+"id"+"\": \"" + x.getId.dropRight("-base_product".length) + "\", \"error\": " + x.getFailureMessage.toJson.toString + "}").mkString(",") + "]"
              val success = "[" + response.getItems.filter(!_.isFailed).map(x => "\"" + x.getId.dropRight("-base_product".length) + "\"").mkString(",") + "]"
              val respStr = "{\"failed\": " + failures + ", \"successful\": " + success + "}"
              if (response.hasFailures) {
                val timeTaken = System.currentTimeMillis - startTime
                warn("[indexing base_product "+string("params.index")+"."+string("params.type")+"] [" + response.getTookInMillis + "/" + timeTaken + "] [" + reqSize + "] [" + response.buildFailureMessage() + "] [" + respStr + "]")
                completer ! IndexFailureResult(parse(respStr))
              }
              else {
                val timeTaken = System.currentTimeMillis - startTime
                info("[indexed base_product "+string("params.index")+"."+string("params.type")+"] [" + response.getTookInMillis + "/" + timeTaken + "] [" + reqSize + "] [" + respStr + "]")
                completer ! IndexSuccessResult(parse(respStr))
              }
            } catch {
              case e: Throwable =>
                val timeTaken = System.currentTimeMillis - startTime
                error("[indexing base_product "+string("params.index")+"."+string("params.type")+"] [" + timeTaken + "] [" + reqSize + "] [" + e.getMessage + "]", e)
                throw e
            }
          }

          override def onFailure(e: Throwable): Unit = {
            val timeTaken = System.currentTimeMillis - startTime
            error("[indexing base_product "+string("params.index")+"."+string("params.type")+"] [" + timeTaken + "] [" + reqSize + "] [" + e.getMessage + "]", e)
            throw e
          }
        })

      } catch {
        case e: Throwable =>
          error("[indexing base_product "+string("params.index")+"."+string("params.type")+"] [" + e.getMessage + "]", e)
          throw e
      }

  }



}
