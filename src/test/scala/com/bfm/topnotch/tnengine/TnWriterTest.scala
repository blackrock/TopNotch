package com.bfm.topnotch.tnengine

import java.net.URL

import com.bfm.topnotch.SparkApplicationTester
import org.scalatest.{Tag, Matchers}

/**
 * The tests for [[com.bfm.topnotch.tnengine.TnWriter TnWriter]].
 */
class TnWriterTest extends SparkApplicationTester with Matchers {

  /**
   * The tags
   */
  object getWriterTag extends Tag("getWriter")

  lazy val fileReader = new TnFileReader
  lazy val engine = new TnEngine(spark)

  "getWriter" should "return an HDFS writer when given a config missing the io namespace" taggedAs (getWriterTag) in {
    engine.getWriter(fileReader.readConfiguration("src/test/resources/com/bfm/topnotch/tnengine/emptyPlan.json")) shouldBe a [TnHDFSWriter]
  }

  it should "return an HDFS writer when given HDFS as the config string with no path" taggedAs (getWriterTag) in {
    engine.getWriter(fileReader.readConfiguration("src/test/resources/com/bfm/topnotch/tnengine/writer/hdfsNoFile.json")) shouldBe a [TnHDFSWriter]
  }

  it should "return an HDFS writer with a non-default destination when given hdfs as the writer with a destination string" taggedAs (getWriterTag) in {
    val writer = engine.getWriter(fileReader.readConfiguration("src/test/resources/com/bfm/topnotch/tnengine/writer/hdfsWithFile.json"))
    writer shouldBe a [TnHDFSWriter]
    writer.asInstanceOf[TnHDFSWriter].dest.get shouldBe "/user/testUser/"
  }

  it should "return an Hbase writer when given HBase as the config string" taggedAs (getWriterTag) in {
    engine.getWriter(fileReader.readConfiguration("src/test/resources/com/bfm/topnotch/tnengine/writer/hbase.json")) shouldBe a [TnHBaseWriter]
  }

  it should "return a REST writer with a non-default URL when given rest as the writer with a destination string" taggedAs (getWriterTag) in {
    val writer = engine.getWriter(fileReader.readConfiguration("src/test/resources/com/bfm/topnotch/tnengine/writer/rest.json"))
    writer shouldBe a [TnRESTWriter]
    writer.asInstanceOf[TnRESTWriter].dest shouldBe "http://www.testurl.com"
  }
}
