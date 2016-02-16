package com.bfm.topnotch.tnassertion

import TNAssertionReportJsonProtocol._
import com.bfm.topnotch.tnengine.TnPersister
import org.apache.spark.sql.catalyst.SqlParser
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import spray.json._

/**
 * The class for running assertions against a dataset and generating reports.
 * @param persister The object to use to persist reports to a location where a UI can read them.
 */
class TnAssertionRunner(persister: TnPersister) {
  import TnAssertionRunner._

  /**
   * Run a set of assertions on a data set, write a report summarizing the results to a persistent location, and return
   * a data set containing all the invalid rows and the reasons why the rows are invalid
   * @param input The input data to run the assertions on
   * @param reportKey The key used to refer to the report when loading it from the UI
   * @param assertions The rules to run against the dataset
   * @return A data set containing every row declared invalid by at least one assertion and the reasons why each row is invalid
   */
  def runAssertions(input: DataFrame, reportKey: String, assertions: Seq[TnAssertionParams]): DataFrame = {
    val totalCount = input.count
    val assertionReports = assertions.map(checkAssertion(_, input, totalCount))

    persister.persistReport(reportKey, assertionReports.toJson.prettyPrint)

    identifyInvalidRows(input, assertions)
  }

  /**
   * Check whether an assertion matches a dataset and generate a report for the assertion
   * @param assertion The assertion to check against the data set
   * @param df The data set, stored as a dataframe, to scan
   * @param totalCount The number of rows in df, precomputed as this function will be called many times
   * @return The report summarizing the results of running the assertion.
   */
  protected[tnassertion] def checkAssertion(assertion: TnAssertionParams, df: DataFrame, totalCount: Long): TnAssertionReport = {
    val badDF = df.filter("not(" + assertion.query + ")").withColumn(INVALID_COL_NAME, lit(assertion.description))
    val badDFCount = badDF.count.toDouble
    TnAssertionReport(assertion.query, assertion.description, assertion.threshold, if (totalCount > 0) badDFCount / totalCount else 0,
      badDF.limit(SAMPLE_NUM))
  }

  /**
   * Produce a data set with a single field for each invalid row containing all the reasons it is invalid.
   * @param input the entire data set, invalid and valid rows
   * @param assertions the assertions to check
   * @return A dataframe containing every invalid row. It contains the row's data and the descriptions of all the assertions
   *         claiming it is invalid.
   */
  protected[tnassertion] def identifyInvalidRows(input: DataFrame, assertions: Seq[TnAssertionParams]): DataFrame = {
    val assertionCols = assertions.map {
      assertion =>
        val expr = SqlParser.parseExpression(assertion.query)
        when(new Column(expr), null).otherwise(assertion.description)
    }

    input
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
}

