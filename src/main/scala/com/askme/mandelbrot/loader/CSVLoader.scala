package com.askme.mandelbrot.loader

import java.io.{BufferedOutputStream, FileOutputStream, BufferedInputStream, FileInputStream}
import java.nio.charset.Charset
import java.util
import java.util.zip.GZIPInputStream
import com.askme.mandelbrot.Configurable
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import scala.collection.JavaConversions._

import scala.io.{Codec, Source}

/**
 * Created by adichad on 04/11/14.
 */


object CSVLoader extends App with Logging with Configurable {
  implicit class `nonempty or else`(val s: String) extends AnyVal {
    def nonEmptyOrElse(other: => String) = if (s.trim.isEmpty) other else s }

  protected[this] val config = configure("placesrc","environment", "application", "environment_defaults", "application_defaults")

  try {
    backFillSystemProperties("component.name", "log.path.current", "log.path.archive", "log.level")

    val sb = new StringBuilder
    val esType = "place"
    val fieldDelim = int("mappings."+esType+".delimiter.field").toChar.toString
    val index = string("mappings."+esType+".destination.index")
    val mapConf = conf("mappings."+esType+".fields")
    val junk = 0.toChar.toString

    val idPos = mapConf.getConfig(string("mappings."+esType+".id")).getInt("pos")
    info(idPos)
    val pos = new util.ArrayList[Int]
    flatten(mapConf, sb, pos)
    val repl = new util.HashMap[Int, String]
    pos.foreach(p => repl.put(p, "${"+p+"}"))
    val template = sb.toString
    sb.setLength(0)
    val input =
      new GZIPInputStream(new BufferedInputStream(new FileInputStream(string("file.input"))))
    val output = new BufferedOutputStream(new FileOutputStream(string("file.output")))

    Source.fromInputStream(input)(Codec.charset2codec(Charset.forName(string("mappings."+esType+".charset.source")))).getLines.foreach {
      line => {

        val cells = line.replace(junk, "").split(fieldDelim, -1)

        sb.append("{ \"index\" : { \"_index\" : \"").append(index)
          .append("\", \"_type\" : \"").append(esType)
          .append("\", \"_id\" : \"").append(cells(idPos)).append("\" } }\n")

        sb.append(template)
        repl.entrySet().foreach {
          e=>sb.replaceAllLiterally(e.getValue, cells(e.getKey))
        }
        sb.append("\n")
        val bytes = sb.toString.getBytes(Charset.forName("UTF-8"))
        output.write(bytes, 0, bytes.length)
        sb.setLength(0)
      }
    }

    output.close()
    input.close()


  } catch {
    case e: Throwable => error(e)
      throw e
  }

  private def flatten(mapConf: Config, sb: StringBuilder, positions: util.ArrayList[Int]): Unit = {
    val mapping = mapConf.root
    sb.append("{ ")
    mapping.keySet.foreach { field =>
      sb.append("\"").append(field).append("\": ")
      mapConf.getAnyRef(field) match {
        case x: java.util.Map[String, AnyRef] => {
          val fconf = mapConf.getConfig(field)
          if(fconf.hasPath("type")) {
            fconf.getString("type") match {
              case "String" => sb.append("\"${").append(fconf.getInt("pos")).append("}\"")
              case "Number" => sb.append("${").append(fconf.getInt("pos")).append("}")
            }
            positions.add(fconf.getInt("pos"))
          } else flatten(fconf, sb, positions)
        }
        case x: java.util.List[AnyRef] => {
          sb.append("[ ")
          mapConf.getConfigList(field).foreach { c =>
            flatten(c, sb, positions)
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
