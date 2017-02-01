package com.bfm.topnotch.tnassertion

import java.net.URL

import com.bfm.topnotch.SparkApplicationTester
import com.bfm.topnotch.TnTestHelper
import java.io.File
import com.bfm.topnotch.tnengine.{TnRESTWriter, TnHBaseWriter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, LongType}
import org.scalatest.{Matchers, Tag}
import org.json4s._
import org.json4s.native.JsonMethods._
import spray.json._

/**
 * The tests for [[com.bfm.topnotch.tnassertion.TnAssertionRunner TnAssertionRunner]].
 */
class TnAssertionRunnerTest extends SparkApplicationTester with Matchers {
  import TNAssertionReportJsonProtocol._
  import TnTestHelper._
  import TnAssertionRunner._
  implicit val formats = DefaultFormats
  lazy val assertionRunner = new TnAssertionRunner(new TnHBaseWriter(Some(hconn)))
  lazy val filledDF = spark.read.json(getClass.getResource("sampleWithValuesDF.json").getFile).cache
  lazy val windowDF = spark.read.json(getClass.getResource("sampleWithWindowsDF.json").getFile).cache
  lazy val correctPostMergeDF = filledDF.withColumn(INVALID_COL_NAME, lit(null).cast(StringType))
  lazy val emptyDFPreMerge = spark.createDataFrame(sc.emptyRDD[Row], filledDF.schema).withColumn(INDEX_COL_NAME, lit(null).cast(LongType)).cache
  lazy val emptyDFPostMerge = emptyDFPreMerge.withColumn(INVALID_COL_NAME, lit(null).cast(StringType)).cache


  /**
   * The tags
   */
  object checkAssertionTag extends Tag("checkAssertion")
  object identifyTag extends Tag("identifyInvalidRows")
  object driverTag extends Tag("TnAssertionRunner")
  object windowTag extends Tag("getSampleWindows")

  /**
   * Load the assertions
   */
  val allIntsAssertion = parse(new File("src/test/resources/com/bfm/topnotch/tnassertion/TnAssertionAllInts.json")).extract[TnAssertionParams]
  val noIntsAssertion = parse(new File("src/test/resources/com/bfm/topnotch/tnassertion/TnAssertionNoInts.json")).extract[TnAssertionParams]
  val not3Assertion = parse(new File("src/test/resources/com/bfm/topnotch/tnassertion/TnAssertionNot3.json")).extract[TnAssertionParams]
  val only1Assertion = parse(new File("src/test/resources/com/bfm/topnotch/tnassertion/TnAssertionOnly1.json")).extract[TnAssertionParams]
  val userDefinedAssertion = parse(new File("src/test/resources/com/bfm/topnotch/tnassertion/TnAssertionUserDefinedStatsAndFeatures.json")).extract[TnAssertionParams]

  /**
   * The tests for checkAssertion
   */
  "checkAssertion" should "return a TnAssertionReport with an empty sampleInvalidData dataframe when given no data" taggedAs checkAssertionTag in {
    val report = assertionRunner.checkAssertion(allIntsAssertion, emptyDFPreMerge, emptyDFPreMerge.count)
    checkRequiredReport(report, allIntsAssertion, 0.0, emptyDFPreMerge)
  }

  it should "return a TnAssertionReport with an empty sampleInvalidData dataframe when all points are invalid and there " +
    "are fewer than SAMPLE_NUM data points" taggedAs checkAssertionTag in {
    val initDF = attachIdx(growDataFrame(filledDF, SAMPLE_NUM - 1))
    val report = assertionRunner.checkAssertion(allIntsAssertion, initDF, initDF.count)
    checkRequiredReport(report, allIntsAssertion, 0.0, emptyDFPreMerge)
  }

  it should "return a TnAssertionReport with an empty sampleInvalidData dataframe when all points are good and there " +
    "are SAMPLE_NUM data points" taggedAs checkAssertionTag in {
    val initDF = attachIdx(growDataFrame(filledDF, SAMPLE_NUM))
    val report = assertionRunner.checkAssertion(allIntsAssertion, initDF, initDF.count)
    checkRequiredReport(report, allIntsAssertion, 0.0, emptyDFPreMerge)
  }

  it should "return a TnAssertionReport with an empty sampleInvalidData dataframe when all points are good and there " +
    "are more than SAMPLE_NUM data points" taggedAs checkAssertionTag in {
    val initDF = attachIdx(growDataFrame(filledDF, SAMPLE_NUM + 1))
    val report = assertionRunner.checkAssertion(allIntsAssertion, initDF, initDF.count)
    checkRequiredReport(report, allIntsAssertion, 0.0, emptyDFPreMerge)
  }

  it should "return a TnAssertionReport identifying 1 point as invalid when there is 1 point and it is invalid" taggedAs checkAssertionTag in {
    val initDF = attachIdx(growDataFrame(filledDF, 1))
    val report = assertionRunner.checkAssertion(noIntsAssertion, initDF, initDF.count)
    checkRequiredReport(report, noIntsAssertion, 1.0, initDF)
  }

  it should "return a correct TnAssertionReport when there are fewer than SAMPLE_NUM points and half are invalid" taggedAs checkAssertionTag in {
    val initDF = attachIdx(growDataFrame(filledDF.filter("Ints = 1 OR Ints = 3"), numDivisibleBy(SAMPLE_NUM - 1, 2)))
    val report = assertionRunner.checkAssertion(only1Assertion, initDF, initDF.count)
    checkRequiredReport(report, only1Assertion, 0.5, initDF.filter("Ints = 3"))
  }

  it should "return a correct TnAssertionReport when there are 2*SAMPLE_NUM points and 0.5*SAMPLE_NUM are invalid" taggedAs checkAssertionTag in {
    val initDF = attachIdx(growDataFrame(filledDF, numDivisibleBy(SAMPLE_NUM*2, 4)))
    val report = assertionRunner.checkAssertion(not3Assertion, initDF, initDF.count)
    checkRequiredReport(report, not3Assertion, 0.25, initDF.filter("Ints = 3"))
  }

  it should "return a correct TnAssertionReport when there are 4*SAMPLE_NUM points and SAMPLE_NUM are invalid" taggedAs checkAssertionTag in {
    val initDF = attachIdx(growDataFrame(filledDF, numDivisibleBy(SAMPLE_NUM*4, 4)))
    val report = assertionRunner.checkAssertion(not3Assertion, initDF, initDF.count)
    checkRequiredReport(report, not3Assertion, 0.25, initDF.filter("Ints = 3"))
  }

  it should "return a correct TnAssertionReport when there are 3*SAMPLE_NUM points and 1.5*SAMPLE_NUM are invalid" taggedAs checkAssertionTag in {
    val initDF = attachIdx(growDataFrame(filledDF.filter("Ints = 1 OR Ints = 3"), numDivisibleBy(SAMPLE_NUM*3, 2)))
    val report = assertionRunner.checkAssertion(only1Assertion, initDF, initDF.count)
    checkRequiredReport(report, only1Assertion, 0.5, initDF.filter("Ints = 3"))
  }

  it should "return a correct TnAssertionReport when there are 3*SAMPLE_NUM points and all are invalid" taggedAs checkAssertionTag in {
    val initDF = attachIdx(growDataFrame(filledDF.filter("Ints = 3"), SAMPLE_NUM*3))
    val report = assertionRunner.checkAssertion(only1Assertion, initDF, initDF.count)
    checkRequiredReport(report, only1Assertion, 1.0, initDF)
  }

  it should "return a TnAssertionReport with user defined summary statistics calculated correctly" taggedAs checkAssertionTag in {
    val report = assertionRunner.checkAssertion(userDefinedAssertion, filledDF, filledDF.count)
    val userStats = report.userDefinedSummaryStats.get
    userStats.count shouldBe 1
    userStats.head.getAs[Double]("maxOfIntsDoublesSum") shouldBe 15.0
    userStats.head.getAs[Int]("avgOfStringsLength") shouldBe 8
  }

  it should "return a TnAssertionReport with user defined features calculated correctly" taggedAs checkAssertionTag in {
    val report = assertionRunner.checkAssertion(userDefinedAssertion, filledDF, filledDF.count)
    val sampleBadData = report.sampleInvalidData.collect

    sampleBadData.foreach(r => {
      r.getAs[Double]("intsDoublesSum") shouldBe r.getAs[Long]("Ints") + r.getAs[Double]("Doubles")
      r.getAs[Int]("stringsLength") shouldBe 8L
    })

    sampleBadData.isEmpty shouldBe false
  }

  it should "return a TnAssertionReport with user defined features recorded for inclusion in the JSON report's sampleInvalid" taggedAs checkAssertionTag in {
    val report = assertionRunner.checkAssertion(userDefinedAssertion, filledDF, filledDF.count)
    report.userDefinedFeatures should contain theSameElementsAs Seq("intsDoublesSum", "stringsLength")
  }

  it should "return a TnAssertionReport with one window per invalid sample if window params are defined" taggedAs checkAssertionTag in {
    val report = assertionRunner.checkAssertion(userDefinedAssertion, filledDF, filledDF.count)
    report.userDefinedFeatures should contain theSameElementsAs Seq("intsDoublesSum", "stringsLength")
  }

  /**
   * The tests for identifyInvalidRows
   */
  "identifyInvalidRows" should "return an empty dataframe of correct schema for no data and no assertions" taggedAs identifyTag in {
    val merged = assertionRunner.identifyInvalidRows(emptyDFPreMerge, Seq())
    dfEquals(merged, emptyDFPostMerge)
  }

  it should "return an empty dataframe of correct schema for some data but no assertions" taggedAs identifyTag in {
    val merged = assertionRunner.identifyInvalidRows(attachIdx(filledDF), Seq())
    dfEquals(merged, emptyDFPostMerge)
  }

  it should "merge correctly for 1 point and 1 assertion with no invalid rows" taggedAs identifyTag in {
    checkMerge(attachIdx(growDataFrame(filledDF, 1)), Seq(allIntsAssertion), lit(""), "false")
  }

  it should "merge correctly for 1 point and 1 assertion with 1 invalid row" taggedAs identifyTag in {
    checkMerge(attachIdx(growDataFrame(filledDF, 1)), Seq(noIntsAssertion), lit(noIntsAssertion.description))
  }

  it should "merge correctly for 1 point and 2 assertions with the 1 invalid row in each" taggedAs identifyTag in {
    checkMerge(attachIdx(filledDF.filter("Ints = 3")), Seq(noIntsAssertion, not3Assertion),
      lit(noIntsAssertion.description + REASON_JOINER + not3Assertion.description))
  }

  it should "merge correctly for 2 points and 2 assertions each no invalid rows in either" taggedAs identifyTag in {
    checkMerge(attachIdx(filledDF.filter("Ints = 1 or Ints = 5")), Seq(allIntsAssertion, not3Assertion), lit(""), "false")
  }

  it should "merge correctly for 2 points and 1 assertion with 1 invalid row" taggedAs identifyTag in {
    checkMerge(attachIdx(filledDF.filter("Ints = 1 OR Ints = 3")), Seq(not3Assertion), lit(not3Assertion.description), "Ints = 3")
  }

  it should "merge correctly for 2 points and 1 assertion with 2 invalid rows" taggedAs identifyTag in {
    checkMerge(attachIdx(filledDF.filter("Ints = 3 OR Ints = 5")), Seq(only1Assertion), lit(only1Assertion.description))
  }

  it should "merge correctly for 2 points and 2 assertions with 1 invalid row for one assertions and 2 invalid rows for the other" taggedAs identifyTag in {
    val assertions = Seq(noIntsAssertion, not3Assertion)
    val descrNoInts = noIntsAssertion.description
    val descrNo3 = descrNoInts + REASON_JOINER + not3Assertion.description
    val failureCol = udf({(int: Int) => if (int == 1) descrNoInts else descrNo3}).apply(new ColumnName("Ints"))
    checkMerge(attachIdx(filledDF.filter("Ints = 1 OR Ints = 3")), assertions, failureCol)
  }

  /**
    * The tests for getSampleWindows
    */

  "getSampleWindows" should "return a none when given a none for windowParams" taggedAs windowTag in {
    assertionRunner.getSampleWindows(windowDF, windowDF, None) shouldBe None
  }

  it should "return a none when no partitioning columns are provided" taggedAs windowTag in {
    assertionRunner.getSampleWindows(windowDF.filter("Success = false"), windowDF,
      Some(TnSampleWindowParams(Seq(), Seq("Order1")))) shouldBe None
  }

  it should "return a none when no windowing columns are provided" taggedAs windowTag in {
    assertionRunner.getSampleWindows(windowDF.filter("Success = false"), windowDF,
      Some(TnSampleWindowParams(Seq("Window1"), Seq()))) shouldBe None
  }

  it should "return a sample window for each invalid point" taggedAs windowTag in {
    val sampleWindowsResult = assertionRunner.getSampleWindows(windowDF.filter("Success = false"), windowDF,
      Some(TnSampleWindowParams(Seq("Window1"), Seq("Order1"))))

    sampleWindowsResult.get.sampleWindows.size shouldBe 3
  }

  it should "return equivalent sample windows of appropriate size for invalid points in the sample window" taggedAs windowTag in {
    val sampleWindowsResult = assertionRunner.getSampleWindows(windowDF.filter("Success = false"), windowDF,
      Some(TnSampleWindowParams(Seq("Window1"), Seq("Order1"))))

    sampleWindowsResult.get.sampleWindows(0).count() shouldBe 3L
    dfEquals(sampleWindowsResult.get.sampleWindows(0), sampleWindowsResult.get.sampleWindows(1))
    dfEquals(sampleWindowsResult.get.sampleWindows(1), sampleWindowsResult.get.sampleWindows(2))
  }

  it should "work with multiple column partitioning, returning different windows when samples fall in different windows" taggedAs windowTag in {
    val sampleWindowsResult = assertionRunner.getSampleWindows(windowDF.filter("Success = false"), windowDF,
      Some(TnSampleWindowParams(Seq("Window1", "Window2"), Seq("Order1"))))

    sampleWindowsResult.get.sampleWindows.size shouldBe 3
    sampleWindowsResult.get.sampleWindows(0).count() shouldBe 2
    sampleWindowsResult.get.sampleWindows(2).count() shouldBe 1
  }

  it should "order windows correctly when provided with one order column" taggedAs windowTag in {
    val sampleWindowsResult = assertionRunner.getSampleWindows(windowDF.filter("Success = false"), windowDF,
      Some(TnSampleWindowParams(Seq("Window1"), Seq("Order1"))))

    sampleWindowsResult.get.sampleWindows.size shouldBe 3
    sampleWindowsResult.get.sampleWindows(0)
      .collect()
      .zipWithIndex
      .filter { case (row, i) => row.getAs[Double]("Order1") != (i + 1) * 2} shouldBe empty
  }

  it should "order windows correctly when provided with two order columns" taggedAs windowTag in {
    val sampleWindowsResult = assertionRunner.getSampleWindows(windowDF.filter("Success != false or Success is null"), windowDF,
      Some(TnSampleWindowParams(Seq("Window1"), Seq("Order1", "Order2"))))

    sampleWindowsResult.get.sampleWindows.size shouldBe 3
    sampleWindowsResult.get.sampleWindows(0)
      .collect()
      .zipWithIndex
      .filter { case (row, i) => row.getAs[Double]("Order1") != Seq(8, 10, 10)(i) &&
        row.getAs[String]("Order1") != ('d' + i).toChar.toString} shouldBe empty
  }

  /**
   * The tests for the entire TnAssertionRunner
   */
  "TnAssertionRunner" should "load local parquet file with 4 rows, run 1 test, and write results to HBase" taggedAs driverTag in {
    runWithMocks(attachIdx(filledDF), Seq(only1Assertion))
  }

  it should "load local parquet file with 4 rows, run 2 tests, and write results to HBase" taggedAs driverTag in {
    runWithMocks(attachIdx(filledDF), Seq(not3Assertion, only1Assertion))
  }

  /**
   * Ensure that the required parts of the report generated from running checkAssertion is correct
 *
   * @param generatedReport The TnAssertionReport created by running checkAssertion
   * @param assertion The assertion used to generate the TnAssertionReport
   * @param intendedPercent The percent of wrong values that the test expects
   * @param invalidPoints All the data points that are supposed to be invalid in the dataset used to create generatedReport
   */
  def checkRequiredReport(generatedReport: TnAssertionReport, assertion: TnAssertionParams, intendedPercent: Double, invalidPoints: DataFrame) {
    // check that TnAssertionReport has the correct values copied directly from TnAssertionParams
    generatedReport.query shouldBe assertion.query
    generatedReport.description shouldBe assertion.description
    generatedReport.threshold shouldBe assertion.threshold

    // check that the percent wrong matches what the test expects
    generatedReport.fractionInvalid shouldBe intendedPercent

    val invalidPointsSet = invalidPoints.select(INDEX_COL_NAME).collect().map(_.getLong(0)).toSet
    // Ensure that SampleFailures is of the correct size, is a subset of all the invalid data points, and has the right schema
    val sampleFailuresSet = generatedReport.sampleInvalidData.select(INDEX_COL_NAME).collect().map(_.getLong(0)).toSet
    if (sampleFailuresSet.size < SAMPLE_NUM) {
      sampleFailuresSet.size shouldBe invalidPoints.count
    }
    else {
      sampleFailuresSet.size shouldBe SAMPLE_NUM
    }
    sampleFailuresSet.subsetOf(invalidPointsSet) shouldBe true
    dfEquals(generatedReport.sampleInvalidData.drop(INVALID_COL_NAME), invalidPoints, true)
  }

  /**
   * Ensure that identifyInvalidRows merges together the reasons correctly
 *
   * @param dfToCheck The initial dataframe whose merged reports we want to check
   * @param assertions The assertions used to generate the reports
   * @param failureCol The text that should be in the reason for failure column, can be derived from a UDF dependent on the other columns in dfToCheck
   * @param mergedRowsFilter A filter for the rows in dfToCheck that should end up in merged report of failures
   */
  def checkMerge(dfToCheck: DataFrame, assertions: Seq[TnAssertionParams], failureCol: Column, mergedRowsFilter: String = "true") {
    val merged = assertionRunner.identifyInvalidRows(dfToCheck, assertions)
    val correctMerged = dfToCheck.filter(mergedRowsFilter).withColumn(INVALID_COL_NAME, failureCol)
    dfEquals(merged, correctMerged)
  }

  /**
   * Run TnAssertionRunner for a given data set with the following assertions and ensure that the correct values are put in
   * the writer
 *
   * @param idxDF The dataframe with the index column applied to run the checks on
   * @param assertions The assertions to check
   */
  def runWithMocks(idxDF: DataFrame, assertions: Seq[TnAssertionParams]): Unit = {
    import TnHBaseWriter._
    val testReportName = "test.Report"
    val correctReports = assertions.map(assertionRunner.checkAssertion(_, idxDF, idxDF.count))
    setHBaseMock(new HTableParams(
      TABLE_NAME,
      Seq(new HPutParams(Bytes.toBytes(testReportName), Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN_QUALIFIER),
        Bytes.toBytes(correctReports.toJson.prettyPrint),
        (actualReports: Array[Byte]) => {correctReports.toJson == new String(actualReports).parseJson}, new String(_))
      )))
    assertionRunner.runAssertions(idxDF, testReportName, assertions)
  }
}