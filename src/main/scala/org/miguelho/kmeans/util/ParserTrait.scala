package org.miguelho.kmeans.util

import org.miguelho.kmeans.model.dataTransformation.Parser
trait ParserTrait {

  lazy val parser: Parser = buildParser()

  def buildParser(): Parser = new Parser
}
