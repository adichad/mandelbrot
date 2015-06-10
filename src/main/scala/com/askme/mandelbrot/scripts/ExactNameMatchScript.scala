package com.askme.mandelbrot.scripts

import java.util

import grizzled.slf4j.Logging
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.elasticsearch.index.fielddata.ScriptDocValues.Strings
import org.elasticsearch.script.{ExecutableScript, NativeScriptFactory, AbstractLongSearchScript}

import scala.collection.JavaConversions._


class ExactNameMatch extends NativeScriptFactory with Logging {

  override def newScript(params: util.Map[String, AnyRef]): ExecutableScript = {
    new ExactNameMatchScript(params.get("name").asInstanceOf[String])
  }
}

class ExactNameMatchScript(name: String) extends AbstractLongSearchScript with Logging {


  override def runAsLong: Long = {
    val analyzer = doc.mapperService().analysisService().analyzer("keyword_analyzer").analyzer()
    val tokenStream = analyzer.tokenStream("LocationNameExact", doc.get("LocationNameExact").asInstanceOf[Strings].getValue)
    tokenStream.reset()
    val termAttribute: CharTermAttribute = tokenStream.addAttribute(classOf[CharTermAttribute])
    tokenStream.incrementToken()
    var ret = if(new String(termAttribute.buffer, 0, termAttribute.length) == name) 0 else 1
    tokenStream.end()
    tokenStream.close()

    if(ret==1) {
      val aliases = doc.get("CompanyAliasesExact").asInstanceOf[Strings].getValues

      if(aliases.size<1)
        return 1

      ret = aliases.map(a=>analyzer.tokenStream("CompanyAliasesExact", a)).map { tokenStream =>
        tokenStream.reset()
        val termAttribute = tokenStream.addAttribute(classOf[CharTermAttribute])
        tokenStream.incrementToken()
        val iret = if(new String(termAttribute.buffer, 0, termAttribute.length) == name) 0 else 1
        tokenStream.end()
        tokenStream.close()
        iret
      }.min
    }
    ret
  }
}
