package com.bfm.topnotch.tnview

import com.bfm.topnotch.SparkApplicationTester
import com.bfm.topnotch.TnTestHelper
import org.scalatest.{Matchers, Tag}

/**
 * The tests for [[com.bfm.topnotch.tnview.TnViewCreator TnViewCreator]].
 */
class TnViewCreatorTest extends SparkApplicationTester with Matchers {
  import TnTestHelper._

  lazy val tnViewCreator = new TnViewCreator(spark)
  lazy val df = spark.read.parquet(getClass.getResource("currentLoans.parquet").getFile).cache

  /**
   * The tags
   */
  object viewCreatorTag extends Tag("TnViewCreator")

  /**
   * The tests for TnViewCreator
   */
  "TnViewCreator" should "do nothing when given Select *" taggedAs viewCreatorTag in {
    dfEquals(tnViewCreator.createView(Seq(df), TnViewParams(Seq("testTable"), "select * from testTable")), df)
  }

  it should "filter out all but the rows with poolNum == 1 when given the SQL where poolNum = 1" taggedAs viewCreatorTag in {
    dfEquals(tnViewCreator.createView(Seq(df), TnViewParams(Seq("testTable"), "select * from testTable where poolNum = 1")),
      df.where("poolNum = 1"))
  }
}