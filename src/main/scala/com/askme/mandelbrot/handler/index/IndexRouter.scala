package com.askme.mandelbrot.handler.index

import java.nio.charset.Charset

import akka.actor.Props
import com.askme.mandelbrot.Configurable
import com.askme.mandelbrot.handler._
import com.askme.mandelbrot.handler.message.IndexParams
import com.typesafe.config.Config
import spray.http.MediaTypes._
import spray.http.{HttpRequest, RemoteAddress, StatusCodes}
import org.mozilla.universalchardet.UniversalDetector

/**
 * Created by adichad on 31/03/15.
 */
case class IndexRouter(val config: Config) extends Router with Configurable {

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
              extract(_.request.entity.data.toByteArray) { rawdata=>
              //entity(as[Array[Byte]]) { rawdata =>
                var detected = false
                var source_charset = charset_source
                val data = if(source_charset == "") {
                  val charsetMatch = detectCharset(rawdata)
                  info("detected charset: "+charsetMatch)
                  source_charset = if (charsetMatch == null) charset_source else charsetMatch
                  detected = charsetMatch != null

                  new String(new String(rawdata, Charset.forName(source_charset)).getBytes(Charset.forName(charset_target)), Charset.forName(charset_target))
                } else {
                  new String(new String(rawdata, Charset.forName(source_charset)).getBytes(Charset.forName(charset_target)), Charset.forName(charset_target))
                }
                respondWithMediaType(`application/json`) {
                  ctx => context.actorOf(Props(classOf[IndexRequestCompleter], service.config, serverContext, ctx,
                    IndexingParams(
                      RequestParams(httpReq, clip, clip.toString),
                      IndexParams(index, esType),
                      RawData(data, source_charset, detected),
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
