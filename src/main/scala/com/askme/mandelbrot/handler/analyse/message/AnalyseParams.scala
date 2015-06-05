package com.askme.mandelbrot.handler.analyse.message

import com.askme.mandelbrot.handler.RequestParams
import com.askme.mandelbrot.handler.message.{RestMessage, IndexParams}

/**
 * Created by adichad on 05/06/15.
 */
case class AnalyseParams(req: RequestParams, idx: IndexParams, keywords: List[String], data: String, analyzers: List[String], startTime: Long) extends RestMessage