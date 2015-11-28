package com.askme.mandelbrot.es.analysis.misc

import org.elasticsearch.index.analysis.AnalysisModule
import org.elasticsearch.plugins.Plugin

/**
 * Created by adichad on 02/11/15.
 */
class RecombiningTokenFilterPlugin extends Plugin {

  override def name = "analysis-recombining"
  override def description = "'recombining' TokenFilter support"

  def onModule(module: AnalysisModule): Unit = {
    module.addProcessor(new RecombiningTokenFilterBinderProcessor)
  }

}
