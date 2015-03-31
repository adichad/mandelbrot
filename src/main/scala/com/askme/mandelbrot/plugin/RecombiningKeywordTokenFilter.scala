package com.askme.mandelbrot.plugin

import org.apache.lucene.analysis.TokenStream
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.Index
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory
import org.elasticsearch.index.settings.IndexSettings

import scala.annotation.meta.field

/**
 * Created by adichad on 30/03/15.
 */


class RecombiningKeywordTokenFilter(override val index: Index,
                                    @(IndexSettings @field) indexSettings: Settings,
                                    override val name: String,
                                    settings: Settings)
  extends AbstractTokenFilterFactory(index, indexSettings, name, settings) {
  override def create(tokenStream: TokenStream): TokenStream = ???
}
