package com.askme.mandelbrot.piper

import java.security.MessageDigest

import com.askme.mandelbrot.server.RootServer
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.index.IndexResponse
import org.json4s.JsonAST.JValue
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConversions._

/**
 * Created by adichad on 08/07/15.
 */
class PlaceSuggestPiper(val config: Config) extends Piper with Logging {
  //val producer = RootServer.defaultContext.kafkaProducer
  private def md5(s: String) =
    MessageDigest.getInstance("MD5").digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString

  override def receive = {
    case doc: JValue =>
      try {
        val city = JArray((doc \ "CitySynonyms").children.map(_.asInstanceOf[JString]) :+ (doc \ "City").asInstanceOf[JString])
        val area = JArray((doc \ "AreaSynonyms").children.map(_.asInstanceOf[JString]) :+ (doc \ "Area").asInstanceOf[JString])
        val coordinates = (doc \ "LatLong").asInstanceOf[JObject]
        val masterid = (doc \ "MasterID").asInstanceOf[JInt]

        val label = (doc \ "LocationName").asInstanceOf[JString].values.trim
        val id = (doc \ "PlaceID").asInstanceOf[JString].values.trim

        val kw: List[String] = ((doc \ "CompanyAliases").children.map(_.asInstanceOf[JString].values.trim).filter(!_.isEmpty) :+ (doc \ "LocationName").asInstanceOf[JString].values.trim) ++
          ((doc \ "Product").children.map(p => (p \ "categorykeywords").children.map(_.asInstanceOf[JString].values.trim).filter(!_.isEmpty)).flatten ++ (doc \ "Product").children.map(p => (p \ "l3category").asInstanceOf[JString].values.trim).filter(!_.isEmpty)) ++
          (doc \ "Product").children.map(p => (p \ "stringattribute").children.map(a => ((a \ "question").asInstanceOf[JString].values, (a \ "answer").children.map(_.asInstanceOf[JString].values.trim).filter(!_.isEmpty)))).flatten.filter(
            att => att._1.trim.toLowerCase().startsWith("brand") || att._1.trim.toLowerCase().startsWith("menu")
          ).map(a => a._2.filter(!_.isEmpty)).flatten

        val suggestBulk = compact(suggestPlace(label, id, masterid, kw, city, area, coordinates, (doc \ "DeleteFlag").asInstanceOf[JInt].values.toInt))

        RootServer.defaultContext.esClient.prepareIndex(string("params.index"), string("params.type")).setId(id).setSource(suggestBulk).execute(new ActionListener[IndexResponse] {
          override def onFailure(e: Throwable): Unit = {
            error("[indexing place suggestion] [" + e.getMessage + "]", e)
          }

          override def onResponse(response: IndexResponse): Unit = {
            info("[indexed place suggestion] [" + response.getId + "]")
          }
        })
        info(suggestBulk)
      } catch {
        case e => error("[indexing place suggestion] [" + e.getMessage + "]", e)
      }

    //producer.send(KeyedMessage[String, String](string("params.topic"), key, key, (doc \ "LocationName").asInstanceOf[JString].values))

        /*
      RootServer.defaultContext.kafkaStreams(string("params.topic")).foreach { stream =>
        stream.iterator().foreach(m=>info("message: ["+m.key()+","+m.message()+":"+m.partition+","+m.offset))
      }
      */
  }

  def suggestPlace(label: String, id: String, masterid: JInt, kw: List[String], city: JValue, area: JValue, coordinates: JValue, deleteFlag: Int) = {
    val payload = JArray(
      List(
        JObject(
          JField("query",
            JObject(
              JField("PlaceID", JString(id))
            )
          ),
          JField("label", JString(label))
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
              JField("tags", JArray(List(JString("outlet"), JString("place"))))
            )
          )
        )
      ),
      JField("payload",
        payload
      ),
      JField("deleted", JInt(deleteFlag)),
      JField("masterid", masterid)
    )

  }

}
