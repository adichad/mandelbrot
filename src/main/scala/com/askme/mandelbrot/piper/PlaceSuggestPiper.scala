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
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import spray.json.DefaultJsonProtocol._
import spray.json._
import scala.collection.JavaConversions._

/**
 * Created by adichad on 08/07/15.
 */
class PlaceSuggestPiper(val config: Config) extends Piper with Logging {
  //val producer = RootServer.defaultContext.kafkaProducer
  private def md5(s: String) =
    MessageDigest.getInstance("MD5").digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString

  val esClient = RootServer.defaultContext.esClient

  private def analyze(esClient: Client, index: String, field: String, text: String): Array[String] =
    new AnalyzeRequestBuilder(esClient.admin.indices, AnalyzeAction.INSTANCE, index, text).setField(field).get().getTokens.map(_.getTerm).toArray

  override def pipe(json: JValue, completer: ActorRef): Unit = {
      val startTime = System.currentTimeMillis()
      try {
        val bulkRequest = RootServer.defaultContext.esClient.prepareBulk
        for(doc <- json.children) {
          val city = JArray((doc \ "CitySynonyms").children.map(_.asInstanceOf[JString]) :+ (doc \ "City").asInstanceOf[JString])
          val area = JArray((doc \ "AreaSynonyms").children.map(_.asInstanceOf[JString]) :+ (doc \ "Area").asInstanceOf[JString])
          val displayCity = (doc \ "City").asInstanceOf[JString]
          val displayArea = (doc \ "Area").asInstanceOf[JString]
          val categories = (doc \ "Product").children.map(p => (p \ "l3category").asInstanceOf[JString].values.trim).filter(!_.isEmpty)
          val coordinates = (doc \ "LatLong").asInstanceOf[JObject]
          val ctype = (doc \ "CustomerType").asInstanceOf[JInt].values.toInt
          val masterid = JString((doc \ "MasterID").asInstanceOf[JInt].values.toString())

          val labelPlace = (doc \ "LocationName").asInstanceOf[JString].values.trim
          val labelSearch = (doc \ "LocationName").asInstanceOf[JString].values.trim + ", "+(doc \ "Area").asInstanceOf[JString].values.trim + ", "+(doc \ "City").asInstanceOf[JString].values.trim
          val id = (doc \ "PlaceID").asInstanceOf[JString].values.trim

          val kw: List[String] = ((doc \ "CompanyAliases").children.map(_.asInstanceOf[JString].values.trim).filter(!_.isEmpty) :+ (doc \ "LocationName").asInstanceOf[JString].values.trim) ++
            (doc \ "Product").children.flatMap(p => (p \ "categorykeywords").children.map(_.asInstanceOf[JString].values.trim).filter(!_.isEmpty)) ++ categories ++
            (doc \ "CuratedTags").children.map(_.asInstanceOf[JString].values.trim).filter(!_.isEmpty) ++
            (doc \ "Product").children.flatMap(p => (p \ "stringattribute").children.map(a => ((a \ "question").asInstanceOf[JString].values, (a \ "answer").children.map(_.asInstanceOf[JString].values.trim).filter(!_.isEmpty))))
              .filter(att => att._1.trim.toLowerCase().startsWith("brand") || att._1.trim.toLowerCase().startsWith("menu"))
              .flatMap(a => a._2.filter(!_.isEmpty))

          val address = doc\"Address"

          val paytype = doc\"PayType"
          val paytag =
            if(paytype == null || paytype==JNull || paytype.asInstanceOf[JInt].values.toInt==0 )
              List(JString("non_pay_outlet"), JString("non_pay"))
            else
              List(JString("pay_outlet"), JString("pay"))

          val callToAction =
            ("did_phone", doc\"LocationDIDNumber") ~
              ("tollfree", doc\"TollFreeNumber") ~
              ("landline", doc\"LocationLandLine") ~
              ("mobile", doc\"LocationMobile") ~
              ("email", doc\"LocationEmail") ~
              ("contact_mobile", doc\"ContactMobile") ~
              ("contact_email", doc\"ContactEmail")

          bulkRequest.add(
            esClient.prepareIndex(string("params.index"), string("params.type"), id)
              .setSource(compact(render(
                suggestPlace(
                  labelPlace,
                  id,
                  masterid,
                  if(ctype>=350) ctype else 1,
                  kw, city, area, coordinates,
                  displayCity, displayArea, address,
                  JArray(categories.map(JString(_))),
                  paytag,
                  callToAction,
                  (doc \ "DeleteFlag").asInstanceOf[JInt].values.toInt))))
          )
          bulkRequest.add(
            esClient.prepareIndex(string("params.index"), string("params.type"), id+"-search")
              .setSource(compact(render(suggestSearch(labelSearch, id, masterid, if(ctype>=350) ctype else 1, city, area, coordinates, displayCity, displayArea, JArray(categories.map(JString(_))), (doc \ "DeleteFlag").asInstanceOf[JInt].values.toInt))))
          )

        }
        val reqSize = bulkRequest.numberOfActions()
        bulkRequest.execute(new ActionListener[BulkResponse] {
          override def onResponse(response: BulkResponse): Unit = {
            try {

              val failures = "[" + response.getItems.filter(_.isFailed).map(x => "{\""+"id"+"\": \"" + x.getId.dropRight(if (x.getId.endsWith("-search")) "-search".length else 0) + "\", \"error\": " + x.getFailureMessage.toJson.toString + "}").mkString(",") + "]"
              val success = "[" + response.getItems.filter(!_.isFailed).map(x => "\"" + x.getId.dropRight(if (x.getId.endsWith("-search")) "-search".length else 0) + "\"").mkString(",") + "]"
              val respStr = "{\"failed\": " + failures + ", \"successful\": " + success + "}"
              if (response.hasFailures) {
                val timeTaken = System.currentTimeMillis - startTime
                warn("[indexing place "+string("params.index")+"."+string("params.type")+"] [" + response.getTookInMillis + "/" + timeTaken + "] [" + reqSize + "] [" + response.buildFailureMessage() + "] [" + respStr + "]")
                completer ! IndexFailureResult(parse(respStr))
              }
              else {
                val timeTaken = System.currentTimeMillis - startTime
                info("[indexed place "+string("params.index")+"."+string("params.type")+"] [" + response.getTookInMillis + "/" + timeTaken + "] [" + reqSize + "] [" + respStr + "]")
                completer ! IndexSuccessResult(parse(respStr))
              }
            } catch {
              case e: Throwable =>
                val timeTaken = System.currentTimeMillis - startTime
                error("[indexing place "+string("params.index")+"."+string("params.type")+"] [" + timeTaken + "] [" + reqSize + "] [" + e.getMessage + "]", e)
                throw e
            }
          }

          override def onFailure(e: Throwable): Unit = {
            val timeTaken = System.currentTimeMillis - startTime
            error("[indexing place "+string("params.index")+"."+string("params.type")+"] [" + timeTaken + "] [" + reqSize + "] [" + e.getMessage + "]", e)
            throw e
          }
        })

      } catch {
        case e: Throwable =>
          error("[indexing place "+string("params.index")+"."+string("params.type")+"] [" + e.getMessage + "]", e)
          throw e
      }


    //producer.send(KeyedMessage[String, String](string("params.topic"), key, key, (doc \ "LocationName").asInstanceOf[JString].values))

        /*
      RootServer.defaultContext.kafkaStreams(string("params.topic")).foreach { stream =>
        stream.iterator().foreach(m=>info("message: ["+m.key()+","+m.message()+":"+m.partition+","+m.offset))
      }
      */
  }

  def suggestSearch(label: String, id: String, masterid: JValue, ctype: Int, city: JValue, area: JValue, coordinates: JValue, displayCity: JValue, displayArea: JValue, categories: JValue, deleteFlag: Int) = {
    val payload = JArray(
      List(
        JObject(
          JField("queries",
            JArray(
              List(
                JObject(
                  JField("type", JString("outlet")),
                  JField("id", JString(id))
                )
              )
            )
          ),
          JField("display",
            JObject(
              JField("label", JString(label)),
              JField("city", displayCity),
              JField("area", displayArea),
              JField("categories", categories),
              JField("type", JString("outlet"))
            )
          )
        )
      )
    )

    JObject(
      JField("id", JString(id)),
      JField("targeting",
        JArray(
          List(
            JObject(
              JField("city", city),
              JField("area", area),
              JField("coordinates", coordinates),
              JField("kw", JArray(List(JString(label)))),
              JField("label", JString(label)),
              JField("tag", JArray(List(JString("search"))))
            )
          )
        )
      ),
      JField("payload", payload),
      JField("deleted", JInt(deleteFlag)),
      JField("groupby", masterid),
      JField("count", JInt(ctype))
    )
  }

  def suggestPlace(label: String, id: String, masterid: JValue, ctype: Int, kw: List[String], city: JValue, area: JValue, coordinates: JValue, displayCity: JValue, displayArea: JValue, address: JValue, categories: JValue, paytag: List[JString], callToAction: JValue, deleteFlag: Int) = {
    val payload = JArray(
      List(
        JObject(
          JField("queries",
            JArray(
              List(
                JObject(
                  JField("type", JString("outlet")),
                  JField("id", JString(id))
                )
              )
            )
          ),
          JField("display",
            JObject(
              JField("label", JString(label)),
              JField("city", displayCity),
              JField("area", displayArea),
              JField("address", address),
              JField("coordinates", coordinates),
              JField("actions", callToAction),
              JField("categories", categories),
              JField("type", JString("outlet"))
            )
          )
        )
      )
    )

    JObject(
      JField("id", JString(id)),
      JField("targeting",
        JArray(
          List(
            JObject(
              JField("city", city),
              JField("area", area),
              JField("coordinates", coordinates),
              JField("kw", JArray(kw.map(JString(_)))),
              JField("label", JString(label)),
              JField("tag", JArray(List(JString("outlet"))::paytag))
            )
          )
        )
      ),
      JField("payload", payload),
      JField("deleted", JInt(deleteFlag)),
      JField("groupby", masterid),
      JField("count", JInt(ctype))
    )

  }

}
