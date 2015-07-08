package com.askme.mandelbrot.es.analysis.stem

import com.askme.mandelbrot.lucene.analysis.stem._
import org.apache.lucene.analysis.TokenStream
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.inject.assistedinject.Assisted
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.Index
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory
import org.elasticsearch.index.settings.IndexSettings
import scala.collection.JavaConversions._

/**
 * Created by adichad on 30/04/15.
 */
class StemmingTokenFilterFactory @Inject()(index: Index, @IndexSettings indexSettings: Settings,
                                                  @Assisted name: String, @Assisted settings: Settings)
  extends AbstractTokenFilterFactory(index, indexSettings, name, settings) {

  override def create(tokenStream: TokenStream): TokenStream = {
    val stemmerType = settings.get("stemmer")
    val minLen = settings.getAsInt("min-len", 3)
    val blackList = new java.util.HashSet[String](settings.getAsArray("exclude", new Array[String](0)).toSeq)
    val bitPos = settings.getAsInt("mark-bit", 0)
    val markStems = settings.getAsBoolean("mark-stems", false)
    val augment = settings.getAsBoolean("augment", false)

    val stemmer: Option[Stemmer] = stemmerType match {
      case "plural"=>Some(new PluralStemmer(blackList, minLen))
      case "participle"=>Some(new ParticipleStemmer(blackList, minLen))
      case _=>None
    }

    if (augment) new AugmentingStemmingTokenFilter(tokenStream, markStems, bitPos, stemmer.get)
    else new ReplacingStemmingTokenFilter(tokenStream, markStems, bitPos, stemmer.get)
  }
}
