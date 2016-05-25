package com.askme.mandelbrot.handler.search.geo.message

import com.askme.mandelbrot.handler.message.RestMessage

/**
 * Created by adichad on 31/03/15.
 */
case class ContainerParams(gids: String, text: String, `type`: String, tags: String, phone_prefix: String) extends RestMessage
case class RelatedParams(gids: String, text: String, `type`: String, tags: String, phone_prefix: String) extends RestMessage
case class FilterParams(gids: String, text: String, `type`: String, tags: String, phone_prefix: String, channel: String) extends RestMessage
