package com.askme.mandelbrot.es.analysis.stem

import org.elasticsearch.index.analysis.AnalysisModule
import org.elasticsearch.plugins.Plugin

/**
  * Created by adichad on 02/11/15.
  */
class StemmingTokenFilterPlugin extends Plugin {

   override def name = "analysis-stemming"
   override def description = "'stemming' TokenFilter support"

   def onModule(module: AnalysisModule): Unit = {
     module.addProcessor(new StemmingTokenFilterBinderProcessor)
   }

 }
