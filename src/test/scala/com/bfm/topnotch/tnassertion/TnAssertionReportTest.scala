package com.bfm.topnotch.tnassertion

import com.bfm.topnotch.SparkApplicationTester
import com.bfm.topnotch.TnTestHelper
import scala.collection.JavaConverters._
import com.bfm.topnotch.tnassertion.TNAssertionReportJsonProtocol._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.{Tag, Matchers}
import spray.json._

/**
 * The tests for [[com.bfm.topnotch.tnassertion.TnAssertionReport TnAssertionReport]].
 */
class TnAssertionReportTest extends SparkApplicationTester with Matchers {
  import TnTestHelper._

  lazy val df = spark.read.json(getClass.getResource("sampleWithValuesDF.json").getFile)
  lazy val nullsDF = spark.read.json(getClass.getResource("sampleWithNullsDF.json").getFile)
  lazy val windowsDF = spark.read.json(getClass.getResource("sampleWithWindowsDF.json").getFile)
  lazy val dfFieldNames = df.columns

  object assertionReportTag extends Tag("assertionReport")

  "TnAssertionReport" should "generate correct JSON when given with no invalid rows" taggedAs assertionReportTag in {
    val emptyDF = spark.createDataFrame(sc.emptyRDD[Row], df.schema)
    val tnReport = TnAssertionReport("Ints != -7", "Description", 0.1, 2.0, 0, emptyDF)
    val testJSON = readResourceFileToJson("TnReportNoBadRows.json", getClass)
    tnReport.toJson shouldBe testJSON
  }

  it should "generate correct JSON with one invalid row" taggedAs assertionReportTag in {
    val failureDF = df.filter("Ints = 1").select("Ints")
    val tnReport = TnAssertionReport("Ints = 1", "Description", 0.1, 2.0, 0, failureDF)
    val testJSON = readResourceFileToJson("TnReportOneBadRow.json", getClass)
    tnReport.toJson shouldBe testJSON
  }

  it should "be case insensitive for column names" taggedAs assertionReportTag in {
    val failureDF = df.filter("Ints = 1").select("Ints")
    val tnReport = TnAssertionReport("iNTs = 1", "Description", 0.1, 2.0, 0, failureDF)
    val testJSON = readResourceFileToJson("TnReportOneBadRowWeirdCasing.json", getClass)
    tnReport.toJson shouldBe testJSON
  }

  it should "generate correct JSON with two invalid rows" taggedAs assertionReportTag in {
    val failureDF = df.filter("Ints < 4").select("Ints")
    val tnReport = TnAssertionReport("Ints < 4", "Description", 0.1, 2.0, 0, failureDF)
    val testJSON = readResourceFileToJson("TnReportTwoBadRows.json", getClass)
    tnReport.toJson shouldBe testJSON
  }

  it should "generate correct JSON with two parts to the where clause" taggedAs assertionReportTag in {
    val failureDF = df.filter("Ints < 4 and Strings like \"Failure%\"").select("Ints", "Strings")
    val tnReport = TnAssertionReport("Ints < 4 and Strings like \"Failure%\"", "Description", 0.1, 2.0, 0, failureDF)
    val testJSON = readResourceFileToJson("TnReportTwoPartWhere.json", getClass)
    tnReport.toJson shouldBe testJSON
  }

  it should "generate correct JSON with user defined summary statistics" taggedAs assertionReportTag in {
    val failureDF = df.filter("Ints < 4").select("Ints")
    val summaryStatsDF = spark.createDataFrame(List(Row(2)).asJava, StructType(Seq(StructField("intAvg", IntegerType, false))))
    val tnReport = TnAssertionReport("Ints < 4", "Description", 0.1, 2.0, 0, failureDF, userDefinedSummaryStats = Some(summaryStatsDF))
    val testJSON = readResourceFileToJson("TnReportUserDefinedSummaryStatistics.json", getClass)
    tnReport.toJson shouldBe testJSON
  }

  it should "include user defined features in the invalidSample" taggedAs assertionReportTag in {
    val failureDF = df.filter("Ints < 4").select("Ints").withColumn("intsMinus1", df.col("Ints") - lit(1))
    val tnReport = TnAssertionReport("Ints < 4", "Description", 0.1, 2.0, 0, failureDF, None, userDefinedFeatures = Seq("intsMinus1"))
    val testJSON = readResourceFileToJson("TnReportUserDefinedFeatures.json", getClass)
    tnReport.toJson shouldBe testJSON
  }

  it should "include user defined features in the invalid sample if checking only for nulls" taggedAs assertionReportTag in {
    val failureDF = nullsDF.filter("BooleansOrNulls is null").select("Ints").withColumn("intsMinus1", nullsDF.col("Ints") - lit(1))
    val tnReport = TnAssertionReport("Ints < 4", "Description", 0.1, 2.0, 0, failureDF, None, userDefinedFeatures = Seq("intsMinus1"))
    val testJSON = readResourceFileToJson("TnReportUserDefinedFeatures.json", getClass)
    tnReport.toJson shouldBe testJSON
  }

  it should "include ordered windows with only the referenced columns for each invalid point" taggedAs assertionReportTag in {
    val failureDF = windowsDF.filter("Success = false").orderBy("Order1").select("Window1", "Order1", "Success")
    val tnReport = TnAssertionReport("Success = true", "Description", 0.5, 2.0, 0, failureDF,
      Some(TnSampleWindowReport(
        TnSampleWindowParams(Seq("Window1"), Seq("Order1")),
        // since the invalid points are in the same window and are the only points in their window,
        // the system should just return the same window 3 times: all the invalid point
        Seq(failureDF, failureDF, failureDF)
      )))
    val testJSON = readResourceFileToJson("TnReportWindowOneColumn.json", getClass)
    tnReport.toJson shouldBe testJSON
  }

  it should "correctly order windows when earlier columns in the dataframe's json representation have a different order " +
    "but the earlier columns are not supposed to be used in the ordering" taggedAs assertionReportTag in {
    val failureDF = windowsDF.filter("Success = false and NotForOrdering > 0").orderBy("Order1").select("Window1", "NotForOrdering", "Order1", "Success")
    val tnReport = TnAssertionReport("Success = true and NotForOrdering <= 0", "Description", 0.5, 2.0, 0, failureDF,
      Some(TnSampleWindowReport(
        TnSampleWindowParams(Seq("Window1"), Seq("Order1")),
        // since the invalid points are in the same window and are the only points in their window,
        // the system should just return the same window 3 times: all the invalid point
        Seq(failureDF, failureDF, failureDF)
      )))
    val testJSON = readResourceFileToJson("TnReportWindowOnlySomeOrderColumns.json", getClass)
    tnReport.toJson shouldBe testJSON
  }
}
