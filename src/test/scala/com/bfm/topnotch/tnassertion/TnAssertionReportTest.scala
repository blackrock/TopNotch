package com.bfm.topnotch.tnassertion

import com.bfm.topnotch.SparkApplicationTester
import com.bfm.topnotch.TnTestHelper
import com.bfm.topnotch.tnassertion.TNAssertionReportJsonProtocol._
import org.apache.spark.sql.Row
import org.scalatest.{Tag, Matchers}
import spray.json._

/**
 * The tests for [[com.bfm.topnotch.tnassertion.TnAssertionReport TnAssertionReport]].
 */
class TnAssertionReportTest extends SparkApplicationTester with Matchers {
  import TnTestHelper._

  lazy val df = sqlContext.read.json(getClass.getResource("sampleDF.json").getFile)
  lazy val dfFieldNames = df.columns

  object assertionReportTag extends Tag("TnAssertionReport")

  "TnAssertionReport" should "generate correct JSON when given with no bad rows" taggedAs assertionReportTag in {
    val emptyDF = sqlContext.createDataFrame(sc.emptyRDD[Row], df.schema)
    val tnReport = TnAssertionReport("", "Description", 0.1, 2.0, emptyDF)
    val testJSON = readResourceFileToJson("TnReportNoBadRows.json", getClass)
    tnReport.toJson shouldBe testJSON
  }

  it should "generate correct JSON with one bad row" taggedAs assertionReportTag in {
    val failureDF = df.filter("Ints = 1").select("Ints")
    val tnReport = TnAssertionReport("Ints = 1", "Description", 0.1, 2.0, failureDF)
    val testJSON = readResourceFileToJson("TnReportOneBadRow.json", getClass)
    tnReport.toJson shouldBe testJSON
  }

  it should "be case insensitive for column names" taggedAs assertionReportTag in {
    val failureDF = df.filter("Ints = 1").select("Ints")
    val tnReport = TnAssertionReport("iNTs = 1", "Description", 0.1, 2.0, failureDF)
    val testJSON = readResourceFileToJson("TnReportOneBadRowWeirdCasing.json", getClass)
    tnReport.toJson shouldBe testJSON
  }

  it should "generate correct JSON with two bad rows" taggedAs assertionReportTag in {
    val failureDF = df.filter("Ints < 4").select("Ints")
    val tnReport = TnAssertionReport("Ints < 4", "Description", 0.1, 2.0, failureDF)
    val testJSON = readResourceFileToJson("TnReportTwoBadRows.json", getClass)
    tnReport.toJson shouldBe testJSON
  }

  it should "generate correct JSON with two parts to the where clause" taggedAs assertionReportTag in {
    val failureDF = df.filter("Ints < 4 and Strings like \"Failure%\"").select("Ints", "Strings")
    val tnReport = TnAssertionReport("Ints < 4 and Strings like \"Failure%\"", "Description", 0.1, 2.0, failureDF)
    val testJSON = readResourceFileToJson("TnReportTwoPartWhere.json", getClass)
    tnReport.toJson shouldBe testJSON
  }
}
