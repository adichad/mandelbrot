package org.elasticsearch.node

import java.util

import org.elasticsearch.Version
import org.elasticsearch.plugins.Plugin
import collection.JavaConversions._

/**
  * Created by adichad on 27/11/15.
  */
object MandelbrotNodeBuilder {
  implicit class NodeBuilderPimp(val nodeBuilder: NodeBuilder) {
    def buildCustom = {
      val settings = nodeBuilder.settings.build()
      val plugins: util.Collection[Class[_ <: Plugin]] =
        settings.getAsArray("plugin.types").map(t=>Class.forName(t).asInstanceOf[Class[Plugin]]).toSeq
      new Node(settings, Version.CURRENT, plugins)
    }

    def nodeCustom = {
      buildCustom.start()
    }
  }
}
