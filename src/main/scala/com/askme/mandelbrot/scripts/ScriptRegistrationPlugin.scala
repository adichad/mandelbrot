package com.askme.mandelbrot.scripts

import org.elasticsearch.plugins.Plugin
import org.elasticsearch.script.ScriptModule

/**
 * Created by adichad on 03/11/15.
 */
class ScriptRegistrationPlugin extends Plugin {

  override def name = "registrar-scripts"
  override def description = "custom scripts registration"

  def onModule(module: ScriptModule): Unit = {
    module.registerScript("geobucket", classOf[GeoBucket])
    module.registerScript("geobucketsuggest", classOf[GeoBucketSuggestions])
    module.registerScript("docscore", classOf[DocScore])
    module.registerScript("docscoreexponent", classOf[DocScoreExponent])
    module.registerScript("customertype", classOf[CustomerTypeBucket])
    module.registerScript("mediacount", classOf[MediaCount])
    module.registerScript("mediacountsort", classOf[MediaCountSort])
    module.registerScript("exactnamematch", classOf[ExactNameMatch])
    module.registerScript("suggestiontransform", classOf[SuggestionTransform])
    module.registerScript("curatedtag", classOf[CuratedTagComparator])
    module.registerScript("randomizer", classOf[RandomBucketComparator])

  }

}