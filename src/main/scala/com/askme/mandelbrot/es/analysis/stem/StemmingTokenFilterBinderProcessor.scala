package com.askme.mandelbrot.es.analysis.stem

import org.elasticsearch.index.analysis.AnalysisModule
import org.elasticsearch.index.analysis.AnalysisModule.AnalysisBinderProcessor.TokenFiltersBindings

/**
  * Created by adichad on 02/11/15.
  */
class StemmingTokenFilterBinderProcessor extends AnalysisModule.AnalysisBinderProcessor {

   override def processTokenFilters(tokenFilterBindings: TokenFiltersBindings): Unit = {
     tokenFilterBindings.processTokenFilter("stemming", classOf[StemmingTokenFilterFactory])
   }
 }
