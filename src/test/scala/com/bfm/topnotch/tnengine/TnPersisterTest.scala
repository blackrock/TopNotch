package com.bfm.topnotch.tnengine

import com.bfm.topnotch.SparkApplicationTester
import com.typesafe.config.ConfigFactory
import org.scalatest.{Tag, Matchers}

/**
 * The tests for [[com.bfm.topnotch.tnengine.TnPersister TnPersister]].
 */
class TnPersisterTest extends SparkApplicationTester with Matchers {

  /**
   * The tags
   */
  object getPersisterTag extends Tag("getPersister")

  lazy val engine = new TnEngine(sqlContext)

  "getPersister" should "return an HDFS persister when given a config missing the topnotchIO namespace" taggedAs (getPersisterTag) in {
    engine.getPersister(ConfigFactory.empty()) shouldBe a[TnHDFSPersister]
  }

  it should "return an HDFS persister when given HDFS as the config string with no path" taggedAs (getPersisterTag) in {
    engine.getPersister(ConfigFactory.load("com/bfm/topnotch/tnengine/persister/hdfsNoFile.json")) shouldBe a [TnHDFSPersister]
  }

  it should "return an HDFS persister with a non-default path when given HDFS as the config string with a path" taggedAs (getPersisterTag) in {
    val persister = engine.getPersister(ConfigFactory.load("com/bfm/topnotch/tnengine/persister/hdfsWithFile.json"))
    persister shouldBe a [TnHDFSPersister]
    persister.asInstanceOf[TnHDFSPersister].hdfsPath shouldBe "/user/testUser/"
  }

  it should "return an Hbase persister when given HBase as the config string" taggedAs (getPersisterTag) in {
    engine.getPersister(ConfigFactory.load("com/bfm/topnotch/tnengine/persister/hbase.json")) shouldBe a [TnHBasePersister]
  }
}
