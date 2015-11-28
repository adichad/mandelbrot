package com.askme.mandelbrot.es.analysis.misc

import org.elasticsearch.index.analysis.AnalysisModule
import org.elasticsearch.index.analysis.AnalysisModule.AnalysisBinderProcessor.TokenFiltersBindings

/**
 * Created by adichad on 02/11/15.
 */
class RecombiningTokenFilterBinderProcessor extends AnalysisModule.AnalysisBinderProcessor {

  override def processTokenFilters(tokenFilterBindings: TokenFiltersBindings): Unit = {
    tokenFilterBindings.processTokenFilter("recombining", classOf[RecombiningTokenFilterFactory])
  }
}
