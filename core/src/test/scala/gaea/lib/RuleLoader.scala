
package gaea.lib

import org.scalatest._

import scala.io.Source

class RuleLoaderSpec extends FlatSpec {

  "A Rule Loader" should "be able to load file" in {
    //println(getClass.getResource("/bmeg.gaea.yaml"))
    //println("Howdy", getClass())
    //val s = getClass.getResource("/bmeg.gaea.yaml").openStream()
    val s = "core/src/test/resources/bmeg.gaea.yml"
    var rules = gaea.lib.GaeaRuleReader.readFile(s)
  }

}