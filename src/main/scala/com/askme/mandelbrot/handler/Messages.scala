package com.askme.mandelbrot.handler

import com.askme.mandelbrot.handler.message.{IndexParams, RestMessage}
import org.json4s.JsonAST.JValue
import spray.http.{HttpRequest, RemoteAddress}


case class RequestParams(httpReq: HttpRequest, clip: RemoteAddress, trueClient: String) extends RestMessage

case class EmptyResponse(reason: String) extends RestMessage

case class IndexingParams(req: RequestParams, idx: IndexParams, data: RawData, startTime: Long) extends RestMessage
case class RawData(data: String) extends RestMessage

case class IndexFailureResult(response: JValue) extends RestMessage
case class IndexSuccessResult(response: JValue) extends RestMessage





