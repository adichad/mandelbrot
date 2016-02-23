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
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.JavaConversions._

/**
 * Created by adichad on 23/02/16.
 */
class GeoSuggestPiper(val config: Config) extends Piper with Logging {
  //val producer = RootServer.defaultContext.kafkaProducer
  private def md5(s: String) =
    MessageDigest.getInstance("MD5").digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString

  val esClient = RootServer.defaultContext.esClient

  private def analyze(esClient: Client, index: String, field: String, text: String): Array[String] =
    new AnalyzeRequestBuilder(esClient.admin.indices, AnalyzeAction.INSTANCE, index, text).setField(field).get().getTokens.map(_.getTerm).toArray

  implicit class GeoJValue(doc: JValue) {

    def label = (doc \ "name").asInstanceOf[JString].values.trim

    def deleted = if((doc \ "archived").asInstanceOf[JBool].values) 1 else 0

    def gid = (doc \ "gid").asInstanceOf[JInt].values.toString()

    def kw = (doc \ "name").asInstanceOf[JString].values.trim +:
      ((doc \ "synonyms").children.map(s=>s.asInstanceOf[JString].values) ++
        (doc \ "containers_dag").children.map(c=>(c\"name").asInstanceOf[JString].values.trim).filter(!_.isEmpty) ++
        (doc \ "containers_dag").children.flatMap(c=>(c\"synonyms").children.map(s=>s.asInstanceOf[JString].values.trim)).filter(!_.isEmpty) ++
        (doc \ "related_list").children.map(c=>(c\"name").asInstanceOf[JString].values.trim).filter(!_.isEmpty) ++
        (doc \ "related_list").children.flatMap(c=>(c\"synonyms").children.map(s=>s.asInstanceOf[JString].values.trim)).filter(!_.isEmpty)
        )

    def containers = (doc \ "containers_dag").children.map( c =>
      ("name" -> (c\"name").asInstanceOf[JString].values) ~
        ("types" -> (c\"types").children.map(t=>t.asInstanceOf[JString].values))
    )

    def related = (doc \ "related_list").children.map( c =>
      ("name" -> (c\"name").asInstanceOf[JString].values) ~
        ("types" -> (c\"types").children.map(t=>t.asInstanceOf[JString].values))
    )
    def center = (doc \ "center").children.map(c=>c.asInstanceOf[JDouble].values)
    def shape = doc \ "shape"
    def count = if(doc.types.contains("city")) 1000 else if(doc.types.contains("area")) 100 else 10

    def tags = if (doc\"tags" == null) List[String]() else (doc\"tags").children.map(t=>t.asInstanceOf[JString].values)
    def types = (doc\"types").children.map(s=>s.asInstanceOf[JString].values)


  }


  override def pipe(json: JValue, completer: ActorRef): Unit = {
    val startTime = System.currentTimeMillis()
    try {
      val bulkRequest = RootServer.defaultContext.esClient.prepareBulk
      for(doc <- json.children) {
        val suggestion: JValue =
          ("id" -> doc.gid) ~
          ("targeting" ->
            List(
              ("kw" -> doc.kw) ~
                ("coordinates" -> doc.center) ~
                ("label" -> doc.label) ~
                ("tag" -> (
                  (
                    if (doc.types.contains("city") || doc.types.contains("area"))
                      List("geo_unified", "geo_city_area")
                    else List("geo_unified")
                    )
                    ++
                    doc.types.map("geo_"+_)
                    ++
                    doc.tags.map("geo_"+_)
                  )
                  )
            )
          ) ~
          ("payload" ->
            List(
              ("queries" ->
                List(
                  ("type" -> "geo") ~
                  ("id" -> doc.gid)
                )
              ) ~
              ("display" ->
                ("label" -> doc.label) ~
                  ("center" -> doc.center) ~
                  //("shape" -> doc.shape) ~
                  ("type" -> doc.types) ~
                  ("containers" -> doc.types) ~
                  ("related" -> doc.related)
              )
            )
          ) ~
          ("deleted" -> doc.deleted) ~
          ("groupby" -> doc.gid) ~
          ("count" -> doc.count)


          bulkRequest.add(
            esClient.prepareIndex(string("params.index"), string("params.type"), doc.gid.values+"-geo")
              .setSource(compact(render(suggestion)))
          )

        }
        val reqSize = bulkRequest.numberOfActions()
        bulkRequest.execute(new ActionListener[BulkResponse] {
          override def onResponse(response: BulkResponse): Unit = {
            val failures = "[" + response.getItems.filter(_.isFailed).map(x => "{\""+"id"+"\": \"" + x.getId.dropRight("-geo".length) + "\", \"error\": " + x.getFailureMessage.toJson.toString + "}").mkString(",") + "]"
            val success = "[" + response.getItems.filter(!_.isFailed).map(x => "\"" + x.getId.dropRight("-geo".length) + "\"").mkString(",") + "]"
            val respStr = "{\"failed\": " + failures + ", \"successful\": " + success + "}"
            try {
              if (response.hasFailures) {
                val timeTaken = System.currentTimeMillis - startTime
                warn("[indexing geo "+string("params.index")+"."+string("params.type")+"] [" + response.getTookInMillis + "/" + timeTaken + "] [" + reqSize + "] [" + response.buildFailureMessage() + "] [" + respStr + "]")
                completer ! IndexFailureResult(parse(respStr))
              }
              else {
                val timeTaken = System.currentTimeMillis - startTime
                info("[indexed geo "+string("params.index")+"."+string("params.type")+"] [" + response.getTookInMillis + "/" + timeTaken + "] [" + reqSize + "] [" + respStr + "]")
                completer ! IndexSuccessResult(parse(respStr))
              }
            } catch {
              case e: Throwable =>
                val timeTaken = System.currentTimeMillis - startTime
                error("[indexing geo "+string("params.index")+"."+string("params.type")+"] [" + timeTaken + "] [" + reqSize + "] [" + e.getMessage + "]", e)
                completer ! IndexFailureResult(parse(respStr))
            }
          }

          override def onFailure(e: Throwable): Unit = {
            val timeTaken = System.currentTimeMillis - startTime
            error("[indexing geo "+string("params.index")+"."+string("params.type")+"] [" + timeTaken + "] [" + reqSize + "] [" + e.getMessage + "]", e)
            completer ! IndexFailureResult(JString("batch failed with exception: "+e.toString))
          }
        })

      } catch {
        case e: Throwable =>
          error("[indexing geo "+string("params.index")+"."+string("params.type")+"] [" + e.getMessage + "]", e)
          completer ! IndexFailureResult(JString("batch failed with exception: "+e.toString))
      }

  }



}
