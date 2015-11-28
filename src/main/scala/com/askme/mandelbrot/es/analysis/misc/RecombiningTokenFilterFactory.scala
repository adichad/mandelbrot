package com.askme.mandelbrot.es.analysis.misc

import com.askme.mandelbrot.lucene.analysis.misc.RecombiningTokenFilter
import org.apache.lucene.analysis.TokenStream
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.inject.assistedinject.Assisted
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.Index
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory
import org.elasticsearch.index.settings.IndexSettingsService

/**
 * Created by adichad on 30/03/15.
 */

class RecombiningTokenFilterFactory @Inject()(index: Index, indexSettingsService: IndexSettingsService,
                                              @Assisted name: String, @Assisted settings: Settings)
  extends AbstractTokenFilterFactory(index, indexSettingsService.getSettings, name, settings) {

  override def create(tokenStream: TokenStream): TokenStream = {
    val separator = settings.get("separator", " ")
    new RecombiningTokenFilter(tokenStream, separator)
  }
}
