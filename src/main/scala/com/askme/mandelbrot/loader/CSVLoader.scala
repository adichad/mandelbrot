package com.askme.mandelbrot.loader

import java.io._
import java.nio.charset.Charset
import java.util.zip.GZIPInputStream

import com.askme.mandelbrot.Configurable
import com.typesafe.config.Config
import dispatch.Defaults.executor
import dispatch.{Http, as, enrichFuture, host, implyRequestHandlerTuple}
import grizzled.slf4j.Logging

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent._
import scala.io.{Codec, Source}
import scala.util.matching.Regex


/**
 * Created by adichad on 04/11/14.
 */


object CSVLoader extends App with Logging with Configurable {

  protected[this] val config = configure("placesrc","environment", "application", "environment_defaults", "application_defaults")

  implicit class `nonempty or else`(val s: String) extends AnyVal {
    def nonEmptyOrElse(other: => String) = if (s.isEmpty) other else s
    def tokenize(regex: Regex): List[List[String]] = regex.findAllIn(s).matchData.map(_.subgroups).toList
  }

  implicit class `replace in StringBuilder`(val sb: StringBuilder) extends AnyVal {

    def appendReplaced(tokens: List[List[String]], map: String=>(Int,String, String, String), vals: IndexedSeq[String]): StringBuilder = {
      val func: (StringBuilder, List[String]) => StringBuilder = (sb, token) => {
        token match {
          case List(_, placeholder: String) => {
            val d = map(placeholder)
            if(d._3=="many") {
              val elems = vals(d._1).nonEmptyOrElse("").split(d._4).filter(!_.trim.isEmpty)
              if(elems.length>0) {
                sb.append("[")
                if (d._2 == "String") {
                  elems.foreach(sb.append("\"").append(_).append("\", "))
                } else {
                  elems.foreach(sb.append(_).append(", "))
                }
                sb.setLength(sb.length - 2)
                sb.append("]")
              } else
                sb.append("[]")
            } else {
              if(d._2=="String") {
                sb.append("\"").append(vals(d._1)).append("\"")
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

  try {
    backFillSystemProperties("component.name", "log.path.current", "log.path.archive", "log.level")

    val sb = new StringBuilder
    val esType = "place"
    val fieldDelim = int("mappings."+esType+".delimiter.field").toChar.toString
    val elemDelim = int("mappings."+esType+".delimiter.element").toChar.toString
    val index = string("mappings."+esType+".destination.index")
    val endpoints = list[String]("mappings."+esType+".destination.endpoints")
    val mapConf = conf("mappings."+esType+".fields")
    val junk = 0.toChar.toString
    val targetCharset = Charset.forName(string("mappings."+esType+".charset.target"))


    val idPos = mapConf.getConfig(string("mappings."+esType+".id")).getInt("pos")
    val valMap = new mutable.HashMap[String, (Int, String, String, String)]

    flatten(mapConf, sb, valMap, elemDelim)
    val template = sb.toString

    val placeholderPattern = "((?:[^$]+|\\$\\{(?!\\d)})+)|(\\$\\{\\d+})".r
    val templateTokens = template.tokenize(placeholderPattern)

    sb.setLength(0)
    val input =
      new GZIPInputStream(new BufferedInputStream(new FileInputStream(new File(string("file.input")))))


    val uri = host(endpoints(0)) / index / esType / "_bulk"
    info(uri)

    Source.fromInputStream(input)(Codec.charset2codec(Charset.forName(string("mappings."+esType+".charset.source")))).getLines().foreach {
      line => {
        val cells = line.replace(junk, "").split(fieldDelim, -1)

        sb.append("{ \"index\" : { \"_index\" : \"").append(index)
          .append("\", \"_type\" : \"").append(esType)
          .append("\", \"_id\" : \"").append(cells(idPos)).append("\" } }\n")
        sb.appendReplaced(templateTokens, valMap, cells).append("\n")
      }
    }
    input.close()
    val postable = sb.toString
    val request = uri << postable
    val bytes = postable.getBytes(targetCharset)
    val output = new BufferedOutputStream(new FileOutputStream(string("file.output")))
    output.write(bytes, 0, bytes.length)
    output.close()
    sb.setLength(0)

    val jmx: Future[String] = Http(request.OK(as.String))
    debug("before future")
    future {
      blocking {
        info("forcing future resolution")
        info(jmx())
      }
    }
    debug("after future")


  } catch {
    case e: Throwable => error(e)
      throw e
  }

  private def flatten(mapConf: Config, sb: StringBuilder, map: mutable.Map[String, (Int, String, String, String)], elemDelim: String): Unit = {
    val mapping = mapConf.root
    sb.append("{ ")
    mapping.keySet.foreach { field =>
      sb.append("\"").append(field).append("\": ")
      mapConf.getAnyRef(field) match {
        case x: java.util.Map[String, AnyRef] => {
          val fconf = mapConf.getConfig(field)
          if(fconf.hasPath("type")) {
            val placeholder = "${"+fconf.getInt("pos")+"}"
            sb.append(placeholder)
            map.put(placeholder, (
              fconf.getInt("pos"),
              fconf.getString("type"),
              (if (!fconf.hasPath("cardinality")||fconf.getString("cardinality") == null) "" else fconf.getString("cardinality")).nonEmptyOrElse("one"),
              (if (!fconf.hasPath("delimiter")||fconf.getInt("delimiter") == null) "" else fconf.getInt("delimiter").toChar.toString).nonEmptyOrElse(elemDelim)
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
          sb.setLength(sb.length - 2 )
          sb.append(" ]")
        }
      }
      sb.append(", ")
    }
    sb.setLength(sb.length - 2 )
    sb.append(" }")
  }


}
