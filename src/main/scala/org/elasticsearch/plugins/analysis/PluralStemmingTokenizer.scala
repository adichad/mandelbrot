package org.elasticsearch.plugins.analysis

/**
 * Created by adichad on 07/11/14.
 */

import java.util

import org.elasticsearch.common.inject.Module
import org.elasticsearch.plugins.AbstractPlugin

class PluralStemmingTokenizer extends AbstractPlugin {
  override def name = "plurals-stemmer"


  override def description(): String = "Porter stemmer toned down to stem only plurals"

  override def modules: util.Collection[Class[_ <: Module]] = {
    //val modules =
    null
  }
}
