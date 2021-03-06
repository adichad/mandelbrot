package com.askme.mandelbrot.handler.index

import java.nio.charset.Charset

import akka.actor.Props
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler._
import com.askme.mandelbrot.handler.message.IndexParams
import com.typesafe.config.Config
import spray.http.MediaTypes._
import spray.http.{HttpRequest, RemoteAddress, StatusCodes}
import org.apache.tika.parser.txt.CharsetDetector
import org.mozilla.universalchardet.UniversalDetector

/**
 * Created by adichad on 31/03/15.
 */
case class IndexRouter(val parentPath:String) extends Router with Configurable {

  private def detectCharsetTika(bytes: Array[Byte]) = {
    val detector = new CharsetDetector
    val charsetMatch = detector.setText(bytes).detect()
    if(charsetMatch == null) null else charsetMatch.getName
  }

  private def detectCharset(bytes: Array[Byte]) = {
    val detector = new UniversalDetector(null)
    detector.handleData(bytes, 0, bytes.length)
    detector.dataEnd()
    val charset = detector.getDetectedCharset
    detector.reset()
    charset
  }

  override def apply(implicit service: MandelbrotHandler) = {
    import service._
    clientIP { (clip: RemoteAddress) =>
      requestInstance { (httpReq: HttpRequest) =>
        path("index" / Segment / Segment ) { (index, esType) =>
          parameters('charset_source.as[String] ? "", 'charset_target.as[String] ? "utf-8") { (charset_source, charset_target) =>
            if (boolean("enabled")) {
              entity(as[String]) { data =>
                respondWithMediaType(`application/json`) {
                  ctx => context.actorOf(Props(classOf[IndexRequestCompleter], service.parentPath, serverContext, ctx,
                    IndexingParams(
                      RequestParams(httpReq, clip, clip.toString),
                      IndexParams(index, esType),
                      RawData(data, "", false),
                      System.currentTimeMillis
                    )))
                }
              }
            } else {
              respondWithMediaType(`application/json`) {
                complete(StatusCodes.MethodNotAllowed, "unsupported operation")
              }
            }
          }
        }
      }
    }
  }

}
