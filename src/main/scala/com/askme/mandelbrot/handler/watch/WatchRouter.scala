package com.askme.mandelbrot.handler.watch

import java.nio.file.Paths

import com.askme.mandelbrot.handler.{MandelbrotHandler, Router}
import com.askme.mandelbrot.loader.MonitorDir
import spray.http.MediaTypes._

/**
 * Created by adichad on 31/03/15.
 */
case object WatchRouter extends Router {

  override def apply(implicit service: MandelbrotHandler, startTime: Long) = {
    import service._
    path("watch") {
      anyParams('dir, 'index, 'type) { (dir, index, esType) =>
        fsActor ! MonitorDir(Paths.get(dir), index, esType)
        respondWithMediaType(`application/json`) {
          complete {
            """{"acknowledged": true}"""
          }
        }
      }
    }
  }
}
