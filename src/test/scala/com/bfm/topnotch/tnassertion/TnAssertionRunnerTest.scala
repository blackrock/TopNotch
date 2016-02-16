package com.bfm.topnotch.tnassertion

import com.bfm.topnotch.SparkApplicationTester
import com.bfm.topnotch.TnTestHelper
import com.bfm.topnotch.tnengine.TnCmdStrings.tnNamespace
import com.bfm.topnotch.tnengine.TnHBasePersister
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, LongType}
import org.scalatest.{Matchers, Tag}
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import spray.json._

/**
 * The tests for [[com.bfm.topnotch.tnassertion.TnAssertionRunner TnAssertionRunner]].
 */
class TnAssertionRunnerTest extends SparkApplicationTester with Matchers {
  import TNAssertionReportJsonProtocol._
  import TnTestHelper._
  import TnAssertionRunner._

  lazy val assertionRunner = new TnAssertionRunner(new TnHBasePersister(Some(hconn)))
  lazy val filledDF = sqlContext.read.json(getClass.getResource("sampleDF.json").getFile).cache
  lazy val correctPostMergeDF = filledDF.withColumn(INVALID_COL_NAME, lit(null).cast(StringType))
  lazy val emptyDFPreMerge = sqlContext.createDataFrame(sc.emptyRDD[Row], filledDF.schema).withColumn(INDEX_COL_NAME, lit(null).cast(LongType)).cache
  lazy val emptyDFPostMerge = emptyDFPreMerge.withColumn(INVALID_COL_NAME, lit(null).cast(StringType)).cache


  /**
   * The tags
   */
  object checkAssertionTag extends Tag("checkAssertion")
  object identifyTag extends Tag("identifyInvalidRows")
  object driverTag extends Tag("TnAssertionRunner")

  /**
   * Load the assertions
   */
  val allIntsAssertion = ConfigFactory.load("com/bfm/topnotch/tnassertion/TnAssertionAllInts.json").as[TnAssertionParams](tnNamespace)
  val noIntsAssertion = ConfigFactory.load("com/bfm/topnotch/tnassertion/TnAssertionNoInts.json").as[TnAssertionParams](tnNamespace)
  val not3Assertion = ConfigFactory.load("com/bfm/topnotch/tnassertion/TnAssertionNot3.json").as[TnAssertionParams](tnNamespace)
  val only1Assertion = ConfigFactory.load("com/bfm/topnotch/tnassertion/TnAssertionOnly1.json").as[TnAssertionParams](tnNamespace)

  /**
   * The tests for checkAssertion
   */
  "checkAssertion" should "return a TnAssertionReport with an empty sampleBadData dataframe when given no data" taggedAs checkAssertionTag in {
    val report = assertionRunner.checkAssertion(allIntsAssertion, emptyDFPreMerge, emptyDFPreMerge.count)
    checkReport(report, allIntsAssertion, 0.0, emptyDFPreMerge)
  }

  it should "return a TnAssertionReport with an empty sampleBadData dataframe when all points are bad and there " +
    "are fewer than SAMPLE_NUM data points" taggedAs checkAssertionTag in {
    val initDF = attachIdx(growDataFrame(filledDF, SAMPLE_NUM - 1))
    val report = assertionRunner.checkAssertion(allIntsAssertion, initDF, initDF.count)
    checkReport(report, allIntsAssertion, 0.0, emptyDFPreMerge)
  }

  it should "return a TnAssertionReport with an empty sampleBadData dataframe when all points are good and there " +
    "are SAMPLE_NUM data points" taggedAs checkAssertionTag in {
    val initDF = attachIdx(growDataFrame(filledDF, SAMPLE_NUM))
    val report = assertionRunner.checkAssertion(allIntsAssertion, initDF, initDF.count)
    checkReport(report, allIntsAssertion, 0.0, emptyDFPreMerge)
  }

  it should "return a TnAssertionReport with an empty sampleBadData dataframe when all points are good and there " +
    "are more than SAMPLE_NUM data points" taggedAs checkAssertionTag in {
    val initDF = attachIdx(growDataFrame(filledDF, SAMPLE_NUM + 1))
    val report = assertionRunner.checkAssertion(allIntsAssertion, initDF, initDF.count)
    checkReport(report, allIntsAssertion, 0.0, emptyDFPreMerge)
  }

  it should "return a TnAssertionReport identifying 1 point as bad when there is 1 point and it is bad" taggedAs checkAssertionTag in {
    val initDF = attachIdx(growDataFrame(filledDF, 1))
    val report = assertionRunner.checkAssertion(noIntsAssertion, initDF, initDF.count)
    checkReport(report, noIntsAssertion, 1.0, initDF)
  }

  it should "return a correct TnAssertionReport when there are fewer than SAMPLE_NUM points and half are bad" taggedAs checkAssertionTag in {
    val initDF = attachIdx(growDataFrame(filledDF.filter("Ints = 1 OR Ints = 3"), numDivisibleBy(SAMPLE_NUM - 1, 2)))
    val report = assertionRunner.checkAssertion(only1Assertion, initDF, initDF.count)
    checkReport(report, only1Assertion, 0.5, initDF.filter("Ints = 3"))
  }

  it should "return a correct TnAssertionReport when there are 2*SAMPLE_NUM points and 0.5*SAMPLE_NUM are bad" taggedAs checkAssertionTag in {
    val initDF = attachIdx(growDataFrame(filledDF, numDivisibleBy(SAMPLE_NUM*2, 4)))
    val report = assertionRunner.checkAssertion(not3Assertion, initDF, initDF.count)
    checkReport(report, not3Assertion, 0.25, initDF.filter("Ints = 3"))
  }

  it should "return a correct TnAssertionReport when there are 4*SAMPLE_NUM points and SAMPLE_NUM are bad" taggedAs checkAssertionTag in {
    val initDF = attachIdx(growDataFrame(filledDF, numDivisibleBy(SAMPLE_NUM*4, 4)))
    val report = assertionRunner.checkAssertion(not3Assertion, initDF, initDF.count)
    checkReport(report, not3Assertion, 0.25, initDF.filter("Ints = 3"))
  }

  it should "return a correct TnAssertionReport when there are 3*SAMPLE_NUM points and 1.5*SAMPLE_NUM are bad" taggedAs checkAssertionTag in {
    val initDF = attachIdx(growDataFrame(filledDF.filter("Ints = 1 OR Ints = 3"), numDivisibleBy(SAMPLE_NUM*3, 2)))
    val report = assertionRunner.checkAssertion(only1Assertion, initDF, initDF.count)
    checkReport(report, only1Assertion, 0.5, initDF.filter("Ints = 3"))
  }

  it should "return a correct TnAssertionReport when there are 3*SAMPLE_NUM points and all are bad" taggedAs checkAssertionTag in {
    val initDF = attachIdx(growDataFrame(filledDF.filter("Ints = 3"), SAMPLE_NUM*3))
    val report = assertionRunner.checkAssertion(only1Assertion, initDF, initDF.count)
    checkReport(report, only1Assertion, 1.0, initDF)
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

  it should "merge correctly for 1 point and 1 assertion with no bad rows" taggedAs identifyTag in {
    checkMerge(attachIdx(growDataFrame(filledDF, 1)), Seq(allIntsAssertion), lit(""), "false")
  }

  it should "merge correctly for 1 point and 1 assertion with 1 bad row" taggedAs identifyTag in {
    checkMerge(attachIdx(growDataFrame(filledDF, 1)), Seq(noIntsAssertion), lit(noIntsAssertion.description))
  }

  it should "merge correctly for 1 point and 2 assertions with the 1 bad row in each" taggedAs identifyTag in {
    checkMerge(attachIdx(filledDF.filter("Ints = 3")), Seq(noIntsAssertion, not3Assertion),
      lit(noIntsAssertion.description + REASON_JOINER + not3Assertion.description))
  }

  it should "merge correctly for 2 points and 2 assertions each no bad rows in either" taggedAs identifyTag in {
    checkMerge(attachIdx(filledDF.filter("Ints = 1 or Ints = 5")), Seq(allIntsAssertion, not3Assertion), lit(""), "false")
  }

  it should "merge correctly for 2 points and 1 assertion with 1 bad row" taggedAs identifyTag in {
    checkMerge(attachIdx(filledDF.filter("Ints = 1 OR Ints = 3")), Seq(not3Assertion), lit(not3Assertion.description), "Ints = 3")
  }

  it should "merge correctly for 2 points and 1 assertion with 2 bad rows" taggedAs identifyTag in {
    checkMerge(attachIdx(filledDF.filter("Ints = 3 OR Ints = 5")), Seq(only1Assertion), lit(only1Assertion.description))
  }

  it should "merge correctly for 2 points and 2 assertions with 1 bad row for one assertions and 2 bad rows for the other" taggedAs identifyTag in {
    val assertions = Seq(noIntsAssertion, not3Assertion)
    val descrNoInts = noIntsAssertion.description
    val descrNo3 = descrNoInts + REASON_JOINER + not3Assertion.description
    val failureCol = udf({(int: Int) => if (int == 1) descrNoInts else descrNo3}).apply(new ColumnName("Ints"))
    checkMerge(attachIdx(filledDF.filter("Ints = 1 OR Ints = 3")), assertions, failureCol)
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
   * Ensure that the report generated from running checkAssertion is correct
   * @param generatedReport The TnAssertionReport created by running checkAssertion
   * @param assertion The assertion used to generate the TnAssertionReport
   * @param intendedPercent The percent of wrong values that the test expects
   * @param badPoints All the data points that are supposed to be incorrect in the dataset used to create generatedReport
   */
  def checkReport(generatedReport: TnAssertionReport, assertion: TnAssertionParams, intendedPercent: Double, badPoints: DataFrame) {
    // check that TnAssertionReport has the correct values copied directly from TnAssertionParams
    generatedReport.query shouldBe assertion.query
    generatedReport.description shouldBe assertion.description
    generatedReport.threshold shouldBe assertion.threshold

    // check that the percent wrong matches what the test expects
    generatedReport.fractionBad shouldBe intendedPercent

    val badPointsSet = badPoints.select(INDEX_COL_NAME).collect().map(_.getLong(0)).toSet
    // Ensure that SampleFailures is of the correct size, is a subset of all the bad data points, and has the right schema
    val sampleFailuresSet = generatedReport.sampleBadData.select(INDEX_COL_NAME).collect().map(_.getLong(0)).toSet
    if (sampleFailuresSet.size < SAMPLE_NUM) {
      sampleFailuresSet.size shouldBe badPoints.count
    }
    else {
      sampleFailuresSet.size shouldBe SAMPLE_NUM
    }
    sampleFailuresSet.subsetOf(badPointsSet) shouldBe true
    dfEquals(generatedReport.sampleBadData.drop(INVALID_COL_NAME), badPoints, true)
  }

  /**
   * Ensure that identifyInvalidRows merges together the reasons correctly
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
   * the persister
   * @param idxDF The dataframe with the index column applied to run the checks on
   * @param assertions The assertions to check
   */
  def runWithMocks(idxDF: DataFrame, assertions: Seq[TnAssertionParams]): Unit = {
    import TnHBasePersister._
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