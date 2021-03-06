package com.askme.mandelbrot.loader

import java.io._
import java.nio.charset.Charset
import java.util.zip.GZIPInputStream

import akka.actor.Actor
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.server.RootServer.SearchContext
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.{BulkRequestBuilder, BulkResponse}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.io.{Codec, Source}
import com.askme.mandelbrot.util.Utils._


/**
 * Created by adichad on 04/11/14.
 */


case class Index(file: File)

object CSVLoader {

  implicit class `replace in StringBuilder`(val sb: StringBuilder) extends AnyVal {

    //'tokens' is all consecutive pairs of tokens created by splitting around the placeholder regex, so starting count from 1, every
    //even token is a placeholder. makes for a fast (linear) replacer
    def appendReplaced(tokens: List[List[String]], map: String => (Int, String, String, String), vals: IndexedSeq[String]): StringBuilder = {
      val func: (StringBuilder, List[String]) => StringBuilder = (sb, token) => {
        token match {
          case List(_, placeholder: String) => {
            val d = map(placeholder)
            if (d._3 == "many") {
              val elems = vals(d._1).nonEmptyOrElse("").split(d._4).filter(!_.trim.isEmpty).map(_.trim)
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

class CSVLoader(val parentPath: String, index: String, esType: String,
                searchContext: SearchContext)
  extends Actor with Logging with Configurable {

  import com.askme.mandelbrot.loader.CSVLoader._

  private val esClient = searchContext.esClient

  class GroupState {
    var id: String = null
    var json: JValue = parse("{}")
    var count = 0
    var groupCount = 0
    var groupDelCount = 0
    var totalCount = 0
    var totalSize = 0
    var totalIndexCount = 0
    var bulkRequest: BulkRequestBuilder = esClient.prepareBulk
    val sb = new StringBuilder
  }


  //assumes that the result is sorted
  private def groupFlush(id: String, del: Boolean, jsonStr: String, index: String, esType: String,
                         sourcePath: String, groupState: GroupState) {

      groupState.totalCount +=1
      if (id == groupState.id) {
        groupState.groupCount += 1
        if(del)
          groupState.groupDelCount += 1
        else {
          groupState.json = groupState.json merge parse(jsonStr)
          groupState.totalSize += jsonStr.size
          groupState.totalIndexCount += 1
        }
      } else {
        // id changed, start of new group

        flush(groupState, false, sourcePath)

        //info(pretty(render(groupState.json)))
        // set group id
        groupState.id = id
        groupState.groupCount = 1
        if (del) {
          groupState.groupDelCount = 1
          groupState.json = parse("{}")
        }
        else {
          groupState.json = parse(jsonStr)
          groupState.totalSize += jsonStr.size
          groupState.totalIndexCount += 1
          groupState.groupDelCount = 0
        }

      }
      groupState.sb.setLength(0)
  }

  private def flush(groupState: GroupState, force: Boolean, path: String) {
    import groupState._
    // if not first row of first group
    if (groupState.id != null) {

        // add previous group to request
        if (groupState.groupDelCount < groupState.groupCount) {
          groupState.bulkRequest.add(
            esClient.prepareIndex(index, esType, groupState.id)
              .setSource(compact(render(groupState.json)))
          )
        }
        else {
          groupState.bulkRequest.add(
            esClient.prepareDelete(index, esType, groupState.id)
          )
        }


      debug(groupState.id + " subdocs: "+(groupState.groupCount - groupState.groupDelCount) + "=("+ groupState.groupCount + "-"+groupState.groupDelCount+")")

      // increment number of groups processed
      groupState.count += 1

      // if batch size is reached or this is delimiting call, flush.
      if (groupState.totalSize >= innerBatchSize || force) {


        esClient.admin().cluster().prepareHealth(index).setWaitForGreenStatus().get()
        info("[" + path + "] " + "sending indexing request[" + groupState.count + "][" + index + "/" + esType + "]["+groupState.totalSize+" chars]: " + groupState.bulkRequest.numberOfActions + " docs")
        bulkRequest.execute(new ActionListener[BulkResponse] {
          override def onResponse(response: BulkResponse): Unit = {
            info( "[" + path + "] " + "failures: " + response.hasFailures)
            val sb = new StringBuilder
            sb.append("{\"failed\": [")
            response.getItems.filter(_.isFailed).take(10).foreach { item =>
              sb.append("{\"").append(item.getId).append("\": \"").append(item.getFailure.getMessage).append("\"}, ")
            }

            if (sb.charAt(sb.length - 2) == ',')
              sb.setLength(sb.length - 2)
            sb.append("]}")
            info("[" + path + "] " + sb.toString)
          }

          override def onFailure(e: Throwable): Unit = {
            error("[" + path + "] exception during bulk indexing", e)
          }
        })

        groupState.totalCount = 0
        groupState.totalSize = 0


          info("[" + path + "] " + "sent indexing request[" + groupState.count + "][" + index + "/" + esType + "]: " + groupState.bulkRequest.numberOfActions + " docs")
          //info("optimizing [" + index + "]")
          //val optFail = esClient.admin.indices.prepareOptimize(index).setMaxNumSegments(2).execute().get().getFailedShards
          //info("optimized [" + index + "]: failed shards: " + optFail)
          //val refFail = esClient.admin.indices.prepareRefresh(index).execute().get().getFailedShards
          //Thread.sleep(30000)
          //info("refreshed [" + index + "]: failed shards: " + refFail)

        groupState.sb.setLength(0)
        groupState.bulkRequest = esClient.prepareBulk
      }
    }

  }

  override def receive = {
    case Index(file) => {
      info("input file received: " + file.getAbsolutePath)

        val input = new GZIPInputStream(new BufferedInputStream(new FileInputStream(file)))
        info("input file opened: " + file.getAbsolutePath)
        val sb = new StringBuilder


        try {
          //esClient.admin.indices.prepareUpdateSettings(index).setSettings(ImmutableSettings.settingsBuilder.put("refresh_interval", "-1").build).get
          //info("disabled refresh")
          val groupState = new GroupState
          Source.fromInputStream(input)(Codec.charset2codec(Charset.forName(string("mappings." + esType + ".charset.source"))))
            .getLines().foreach {
            line => {

              val cells = line.split(fieldDelim, -1)

                //assumes that the result is sorted
                groupFlush(cells(idPos), cells(delPos).toInt != 0, groupState.sb.appendReplaced(templateTokens, valMap, cells).toString, index, esType, file.getAbsolutePath, groupState)
            }
          }

          flush(groupState, true, file.getAbsolutePath)
//          info("optimizing: "+index)
//          val optResponse = esClient.admin.indices.prepareOptimize(index).setMaxNumSegments(1).get()
//          info("optimized: "+index+", failures: "+ optResponse.getShardFailures.toSet.toString)
          //esClient.admin.cluster.prepareHealth(index).setWaitForGreenStatus.get

        } catch {
          case e: Exception => error("error processing input file: " + file.getAbsolutePath, e)
        } finally {
          input.close()
          info("input file closed: " + file.getAbsolutePath)
          //esClient.admin.indices.prepareUpdateSettings(index).setSettings(ImmutableSettings.settingsBuilder.put("refresh_interval", "120s").build).get
          //info("re-enabled refresh: 120s")
        }


    }
  }

  var innerBatchSize = 50000000

  val fieldDelim = int("mappings." + esType + ".delimiter.field").toChar.toString
  val elemDelim = int("mappings." + esType + ".delimiter.element").toChar.toString
  val mapConf = conf("mappings." + esType + ".fields")
  val targetCharset = Charset.forName(string("mappings." + esType + ".charset.target"))

  val idPos = mapConf.getConfig(string("mappings." + esType + ".id")).getInt("pos")
  val delPos = int("mappings." + esType + ".delete")
  val valMap = new mutable.HashMap[String, (Int, String, String, String)]

  private val sb = new StringBuilder
  flatten(mapConf, sb, valMap, elemDelim)
  val template = sb.toString
  sb.setLength(0)

  val placeholderPattern = "((?:[^$]+|\\$\\{(?!\\d)})+)|(\\$\\{\\d+})".r
  val templateTokens = template.tokenize(placeholderPattern)

  private def flatten(mapConf: Config, sb: StringBuilder, map: mutable.Map[String, (Int, String, String, String)], elemDelim: String): Unit = {
    val mapping = mapConf.root
    sb.append("{ ")
    mapping.keySet.foreach { field =>
      sb.append("\"").append(field).append("\": ")
      mapConf.getAnyRef(field) match {
        case x: java.util.Map[_, _] => {
          val fconf = mapConf.getConfig(field)
          if (fconf.hasPath("type")) {
            val placeholder = "${" + fconf.getInt("pos") + "}"
            sb.append(placeholder)
            map.put(placeholder, (
              fconf.getInt("pos"),
              fconf.getString("type"),
              (if (!fconf.hasPath("cardinality")) "" else fconf.getString("cardinality")).nonEmptyOrElse("one"),
              (if (!fconf.hasPath("delimiter")) "" else fconf.getInt("delimiter").toChar.toString).nonEmptyOrElse(elemDelim)
              )
            )
          } else flatten(fconf, sb, map, elemDelim)
        }
        case x: java.util.List[_] => {
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
