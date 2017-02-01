package com.bfm.topnotch.tnassertion

import com.bfm.topnotch.tnassertion.TNAssertionReportJsonProtocol._
import com.bfm.topnotch.tnengine.TnWriter
import com.typesafe.scalalogging.StrictLogging
//import org.apache.spark.sql.catalyst.SqlParser
import scala.collection.JavaConversions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Row}
import spray.json._


/**
  * The class for running assertions against a dataset and generating reports.
  *
  * @param writer The object to use to persist reports to a location where a UI can read them.
  */
class TnAssertionRunner(writer: TnWriter) extends StrictLogging {
  import TnAssertionRunner._

  /**
    * Run a set of assertions on a data set, write a report summarizing the results to a persistent location, and return
    * a data set containing all the invalid rows and the reasons why the rows are invalid
    *
    * @param input The input data to run the assertions on
    * @param reportKey The key used to refer to the report when loading it from the UI
    * @param assertions The rules to run against the dataset
    * @return An AssertionReturn object containing a data set containing every row declared invalid by at least one
    *         assertion, the reasons why each row is invalid, and an int of how many assertions failed past threshold.
    */
  def runAssertions(input: DataFrame, reportKey: String, assertions: Seq[TnAssertionParams]): AssertionReturn = {
    val totalCount = input.count
    val assertionReports = assertions.map(checkAssertion(_, input, totalCount))
    writer.writeReport(reportKey, assertionReports.toJson.prettyPrint)
    logger.info(s"$reportKey has value \n${assertionReports.toJson.prettyPrint}")
    AssertionReturn(identifyInvalidRows(input, assertions),
      assertionReports.filter(report => report.fractionInvalid > report.threshold).length)
  }

  /**
    * Check whether an assertion matches a dataset and generate a report for the assertion
    *
    * @param assertion The assertion to check against the data set
    * @param df The data set, stored as a dataframe, to scan
    * @param totalCount The number of rows in df, precomputed as this function will be called many times
    * @return The report summarizing the results of running the assertion.
    */
  protected[tnassertion] def checkAssertion(assertion: TnAssertionParams, df: DataFrame, totalCount: Long): TnAssertionReport = {
    val withUserDefinedFeaturesDF = assertion.userDefinedFeatures match {
      case None => df
      case Some(features) => df.selectExpr(("*") +: features.map(nameExprPairToSelectExpr).toSeq: _*)
    }

    val invalidDF = withUserDefinedFeaturesDF.filter("not(" + assertion.query  + ")").withColumn(INVALID_COL_NAME, lit(assertion.description))
    val invalidDFCount = invalidDF.count.toDouble

    val userDefinedSummaryStats = assertion.userDefinedSummaryExpr match {
      case None => None
      case Some(statsNameFuncMap) => Some(invalidDF.selectExpr(statsNameFuncMap.map(nameExprPairToSelectExpr).toSeq : _*))
    }

    val invalidSamplesDF = invalidDF.limit(SAMPLE_NUM).cache

    TnAssertionReport(assertion.query, assertion.description, assertion.threshold, if (totalCount > 0) invalidDFCount / totalCount else 0,
      invalidDFCount.toInt, invalidSamplesDF, getSampleWindows(invalidSamplesDF, df, assertion.sampleWindowParams),
      userDefinedSummaryStats, assertion.userDefinedFeatures.getOrElse(Map[String, String]()).keys.toSeq)
  }

  /**
    * Gets an ordered window of data providing context for each sample invalid point. The data set is partitioned into windows
    * based on values in columns specified by the user. The order of the rows in each window is defined by the columns
    * provided by the user.
    *
    * @param invalidSamplesDF The dataframe containing a sample of invalid points. A window will be returned for each
    *                         row in this dataframe.
    * @param allDF The dataframe of all points, not just those that are invalid. The windows will be taken from here.
    * @param windowParams The columns for partitioning and ordering the windows.
    * @return None if windowParams is none or if either the partitioning or ordering sequences of columns are empty.
    *         Otherwise, a some of a seq of dataframes where each one is the ordered window for a sample point. The
    *         sequence will be ordered so that the ith dataframe corresponds to the ith sample in invalidSamplesDF.
    */
  def getSampleWindows(invalidSamplesDF: DataFrame, allDF: DataFrame, windowParams: Option[TnSampleWindowParams]): Option[TnSampleWindowReport] = {
    windowParams match {
      case Some(params) if params.idsForWindowPartitioning.isEmpty || params.orderEachWindowBy.isEmpty => None
      case Some(params) => {
        val windowKeyToRowsMap = invalidSamplesDF.select(params.idsForWindowPartitioning.map(col): _*)
          // remove samples with the same values in the partitioning columns. Otherwise, on a join, if two samples are
          // in the same window, then all values in that window will appear twice, once for each sample used in the join
          .dropDuplicates()
          // inner join gets the rows that are in the window of any sample
          .join(allDF, params.idsForWindowPartitioning, "inner")
          // can groupby with any type instead of using correct type of each column as we don't need to actually examine
          // the values, we just need an equality operator to determine unique keys. Any gives the comparator
          .collect().groupBy(row => params.idsForWindowPartitioning.map(str => row.getAs[Any](str)))
          .map { case (k, rArr) =>
            // the schema for all rows is the same, so can take it for 1 of them
            k -> allDF.sqlContext.createDataFrame(rArr.toList, rArr(0).schema)
              .orderBy(params.orderEachWindowBy.map(col): _*)
          }

        val windowsOrderedAccordingToSamples = invalidSamplesDF
          .collect().map(row => params.idsForWindowPartitioning.map(str => row.getAs[Any](str)))
          .map{ case rowAsKey => windowKeyToRowsMap.get(rowAsKey).get }.toSeq

        Some(TnSampleWindowReport(params, windowsOrderedAccordingToSamples))
      }
      case None => None
    }
  }

  /**
    * Produce a data set with a single field for each invalid row containing all the reasons it is invalid.
    *
    * @param input the entire data set, invalid and valid rows
    * @param assertions the assertions to check
    * @return A dataframe containing every invalid row. It contains the row's data and the descriptions of all the assertions
    *         claiming it is invalid.
    */
  protected[tnassertion] def identifyInvalidRows(input: DataFrame, assertions: Seq[TnAssertionParams]): DataFrame = {
    var newDF = input
    val assertionCols = assertions.map {
      assertion => {
        newDF =
          if (assertion.userDefinedFeatures.isEmpty) {
            newDF
          }
          else {
            newDF.selectExpr(("*") +: assertion.userDefinedFeatures.get.map(nameExprPairToSelectExpr).toSeq: _*)
          }
        when(expr(assertion.query), null).otherwise(assertion.description)
      }
    }
    newDF
      .withColumn(INVALID_COL_NAME, concat_ws(REASON_JOINER, assertionCols: _*))
      .filter(s"$INVALID_COL_NAME != ''")
  }
}

/**
  * Constants used by the TnAssertionRunner
  */
object TnAssertionRunner {
  // The number of invalid rows to put in each report
  val SAMPLE_NUM = 20
  // need UUID for invalid column name as don't want to conflict with names of columns in data set
  val INVALID_COL_NAME = "__REASON_INVALID__"
  // The string to use to join assertions' reasons that a row is invalid. If multiple assertions declare a row to be
  // invalid, we include all the reasons in the returned dataframe.
  val REASON_JOINER = " &&& "

  /**
    * convert a name, sql select expression pair to a sql select strings with as for rename
    *
    * @param pair the name, sql select expression pair
    * @return The string for the select expression
    */
  def nameExprPairToSelectExpr (pair: (String, String)): String = pair match { case (name, expr) => expr + " as "  + name }
}

/**
  * Case class to store the DataFrame and number of failed assertions returns from runAssertion.
  * @param df DataFrame to be returned from runAssertions
  * @param numFailed number of failed assertions
  */
case class AssertionReturn(df: DataFrame, numFailed: Int) {}

