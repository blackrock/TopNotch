package com.bfm.topnotch.tnengine

import org.json4s._
import org.json4s.native.Serialization
import com.bfm.topnotch.SparkApplicationTester
import org.scalatest.{Matchers, Tag}

/**
 * The tests for [[com.bfm.topnotch.tnengine.TnReader TnReader]].
  * Note that most testing is done by the tests for [[com.bfm.topnotch.tnengine.TnEngine TnEngine]]
 */
class TnReaderTest extends SparkApplicationTester with Matchers {

  object getReaderTag extends Tag("getReader")
  object readerVariableTag extends Tag("readerVariables")
  implicit val formats = Serialization.formats(NoTypeHints)

  "getReader" should "throw an IllegalArgumentException when the plan doesn't exist" taggedAs(getReaderTag) in {
    intercept[IllegalArgumentException] {
      val fileReader = new TnFileReader
      fileReader.readConfiguration("src/test/resources/com/bfm/DOESNTEXIST")
    }
  }

  it should "replace variables in a configuration" taggedAs(readerVariableTag) in {
    val fileReader = new TnFileReader(Map("var1" -> "true", "var2" -> "false"))
    val replacedAST = fileReader.readConfiguration("src/test/resources/com/bfm/topnotch/tnengine/cliReplacementTest.json")
    replacedAST \ "trueToBeReplaced" should not equal(JNothing)
    (replacedAST \ "replaceThisValue").extract[String] should equal("false")
  }
}
