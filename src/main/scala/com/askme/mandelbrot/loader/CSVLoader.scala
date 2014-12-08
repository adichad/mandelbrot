package com.askme.mandelbrot.loader

import java.io._
import java.nio.charset.Charset
import java.util.zip.GZIPInputStream

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import dispatch._
import grizzled.slf4j.Logging
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.{BulkRequestBuilder, BulkResponse}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent._
import scala.io.{Codec, Source}
import scala.util.matching.Regex


/**
 * Created by adichad on 04/11/14.
 */


case class Index(file: File)

object CSVLoader {
  val specials: Seq[(String, String)] = ("\\", "\\\\") +: (("\"", "\\\"") +:
    (for (c <- (0x0000 to 0x001F)) yield (c.toChar.toString, "\\u" + ("%4s" format Integer.toHexString(c)).replace(" ", "0"))))

  implicit class `string utils`(val s: String) extends AnyVal {
    def nonEmptyOrElse(other: => String) = if (s.isEmpty) other else s

    def tokenize(regex: Regex): List[List[String]] = regex.findAllIn(s).matchData.map(_.subgroups).toList

    def escapeJson: String = {
      var res = s
      specials.foreach {
        c => res = res.replace(c._1, c._2)
      }
      res
    }
  }

  implicit class `replace in StringBuilder`(val sb: StringBuilder) extends AnyVal {

    //'tokens' is all consecutive pairs of tokens created by splitting around the placeholder regex, so starting count from 1, every
    //even token is a placeholder. makes for a fast (linear) replacer
    def appendReplaced(tokens: List[List[String]], map: String => (Int, String, String, String), vals: IndexedSeq[String]): StringBuilder = {
      val func: (StringBuilder, List[String]) => StringBuilder = (sb, token) => {
        token match {
          case List(_, placeholder: String) => {
            val d = map(placeholder)
            if (d._3 == "many") {
              val elems = vals(d._1).nonEmptyOrElse("").split(d._4).filter(!_.trim.isEmpty)
              if (elems.length > 0) {
                sb.append("[")
                if (d._2 == "String") {
                  elems.foreach(x => sb.append("\"").append(x.escapeJson).append("\", "))
                } else {
                  elems.foreach(sb.append(_).append(", "))
                }
                sb.setLength(sb.length - 2)
                sb.append("]")
              } else
                sb.append("[]")
            } else {
              if (d._2 == "String") {
                sb.append("\"").append(vals(d._1).escapeJson).append("\"")
              } else {
                sb.append(vals(d._1))
              }
            }

          }
          case List(other: String, _) => sb.append(other)
        }
        sb
      }
      tokens./:(sb)(func)
    }
  }

}

class CSVLoader(val config: Config, index: String, esType: String,
                implicit val executionContext: ExecutionContext, searchContext: SearchContext)
  extends Actor with Logging with Configurable {

  import com.askme.mandelbrot.loader.CSVLoader._

  private val esClient = searchContext.esClient

  class GroupState {
    var id: String = null
    var json: JValue = parse("{}")
  }

  //assumes that the result is sorted
  private val groupFlush: (String, String, String, String, BulkRequestBuilder, GroupState) => Unit = {
    (id, jsonStr, index, esType, req, groupState) =>
      if (id == groupState.id) {
        groupState.json = groupState.json merge parse(jsonStr)
      } else {
        req.add(
          esClient.prepareIndex(index, esType, groupState.id)
            .setSource(compact(render(groupState.json)))
        )
        //info(pretty(render(groupState.json)))
        groupState.id = id
        groupState.json = parse(jsonStr)
      }
  }

  private def getId(cells: Array[String]) = "U"+cells(2)+"L"+cells(4)

  override def receive = {
    case Index(file) => {
      info("input file received: " + file.getAbsolutePath)
      future {
        val input = new GZIPInputStream(new BufferedInputStream(new FileInputStream(file)))
        info("input file opened: " + file.getAbsolutePath)
        val sb = new StringBuilder
        var count = 0
        val bulkRequest = esClient.prepareBulk
        try {
          val groupState = new GroupState
          Source.fromInputStream(input)(Codec.charset2codec(Charset.forName(string("mappings." + esType + ".charset.source"))))
            .getLines().foreach {
            line => {
              val cells = line.split(fieldDelim, -1)
              cells(idPos) = getId(cells)
              //assumes that the result is sorted
              groupFlush(cells(idPos), sb.appendReplaced(templateTokens, valMap, cells).toString, index, esType, bulkRequest, groupState)
              sb.setLength(0)
              count += 1
            }
          }
          groupFlush(null, "{}", index, esType, bulkRequest, groupState)
        } catch {
          case e: Exception => error("error processing input file: " + file.getAbsolutePath, e)
        } finally {
          input.close()
          info("input file closed: " + file.getAbsolutePath)
        }
        info("indexing " + bulkRequest.numberOfActions + " docs from input file: " + file.getAbsolutePath)
        bulkRequest.execute(new ActionListener[BulkResponse] {
          override def onResponse(response: BulkResponse) = {
            info("failures: " + response.hasFailures)
            sb.append("{\"failed\": [")
            response.getItems.foreach { item =>
              if (item.isFailed) {
                sb.append("{\"").append(item.getId).append("\": \"").append(item.getFailure.getMessage).append("\"}, ")
              }
            }
            if (sb.charAt(sb.length - 2) == ',')
              sb.setLength(sb.length - 2)
            sb.append("]}")
            info(sb.toString)
            sb.setLength(0)
          }

          override def onFailure(e: Throwable) = {
            error(e.getMessage, e)
          }

        })
      }
    }
  }

  val fieldDelim = int("mappings." + esType + ".delimiter.field").toChar.toString
  val elemDelim = int("mappings." + esType + ".delimiter.element").toChar.toString
  val endpoints = list[String]("mappings." + esType + ".destination.endpoints")
  val mapConf = conf("mappings." + esType + ".fields")
  val targetCharset = Charset.forName(string("mappings." + esType + ".charset.target"))

  val idPos = mapConf.getConfig(string("mappings." + esType + ".id")).getInt("pos")
  val valMap = new mutable.HashMap[String, (Int, String, String, String)]

  private val sb = new StringBuilder
  flatten(mapConf, sb, valMap, elemDelim)
  val template = sb.toString
  sb.setLength(0)

  val placeholderPattern = "((?:[^$]+|\\$\\{(?!\\d)})+)|(\\$\\{\\d+})".r
  val templateTokens = template.tokenize(placeholderPattern)

  val uri = host(endpoints(0)) / index / esType / "_bulk"
  info(uri)

  private def flatten(mapConf: Config, sb: StringBuilder, map: mutable.Map[String, (Int, String, String, String)], elemDelim: String): Unit = {
    val mapping = mapConf.root
    sb.append("{ ")
    mapping.keySet.foreach { field =>
      sb.append("\"").append(field).append("\": ")
      mapConf.getAnyRef(field) match {
        case x: java.util.Map[String, AnyRef] => {
          val fconf = mapConf.getConfig(field)
          if (fconf.hasPath("type")) {
            val placeholder = "${" + fconf.getInt("pos") + "}"
            sb.append(placeholder)
            map.put(placeholder, (
              fconf.getInt("pos"),
              fconf.getString("type"),
              (if (!fconf.hasPath("cardinality") || fconf.getString("cardinality") == null) "" else fconf.getString("cardinality")).nonEmptyOrElse("one"),
              (if (!fconf.hasPath("delimiter") || fconf.getInt("delimiter") == null) "" else fconf.getInt("delimiter").toChar.toString).nonEmptyOrElse(elemDelim)
              )
            )
          } else flatten(fconf, sb, map, elemDelim)
        }
        case x: java.util.List[AnyRef] => {
          sb.append("[ ")
          mapConf.getConfigList(field).foreach { c =>
            flatten(c, sb, map, elemDelim)
            sb.append(", ")
          }
          sb.setLength(sb.length - 2)
          sb.append(" ]")
        }
      }
      sb.append(", ")
    }
    sb.setLength(sb.length - 2)
    sb.append(" }")
  }


}
