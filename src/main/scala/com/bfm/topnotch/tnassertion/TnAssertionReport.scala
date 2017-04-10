package com.bfm.topnotch.tnassertion

import org.antlr.runtime.tree.Tree
import org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TABLE_OR_COL
import org.apache.hadoop.hive.ql.parse.{ParseDriver, ParseException}
import org.apache.spark.sql.{Column, DataFrame}
import org.json4s._
import org.json4s.native.JsonMethods._

/**
  * A report containing the results of checking the many assertions in one assertion group
  * (assertion group = all the assertions in an assertion command)
  *
  * @param outputKey The outputKey of the command in the plan
  * @param assertionReports The reports of all the assertions in the group
  */
case class TnAssertionGroupReport(outputKey: String, assertionReports: Seq[TnAssertionReport])

/**
  * A report on the results of checking an assertion against a data set
  *
  * @param query The assertion's filter expression for separating valid and invalid data
  * @param description A description of the rule
  * @param threshold The maximum percent of invalid data allowed by the rule before it is considered a failed rule
  * @param fractionInvalid The percent data that failed the rule
  * @param numInvalid The number of rows for which the assertion was false.
  * @param sampleInvalidData A sample of the rows that are invalid according to the rule.
  * @param sampleWindowReport The windows of rows collected around each invalid row in the sample.
  * @param userDefinedSummaryStats Summary statistics of the data defined by the user. Each statistic is a single number,
  *                                such as average value of a column.
  * @param userDefinedFeatures The user defined features to include in sampleInvalid regardless of whether the query references them
  */
case class TnAssertionReport(
                              query: String,
                              description: String,
                              threshold: Double,
                              fractionInvalid: Double,
                              numInvalid: Int = 0,
                              sampleInvalidData: DataFrame,
                              sampleWindowReport: Option[TnSampleWindowReport] = None,
                              userDefinedSummaryStats: Option[DataFrame] = None,
                              userDefinedFeatures: Seq[String] = Seq()
                            )

/**
  * Windows of data providing context for each value in sampleInvalidData. If the user provides a way to partition and
  * order the data, TopNotch will provide the ordered partition for each invalid example.
  * @param sampleWindowParams The parameters used partition and order the data for generating the sample windows.
  *                           This is needed in the report to determine the relevant columns for the dataframes
  *                           in the report.
  * @param sampleWindows The windows of data surrounding each example. In order for TopNotch to work, when a window report
  *                      is created, the sampleWindows seq must be ordered so that the ith window corresponds with
  *                      the ith example in sampleInvalidData.
  */
case class TnSampleWindowReport(
                                 sampleWindowParams: TnSampleWindowParams,
                                 sampleWindows: Seq[DataFrame]
                               )

class TnAssertionReportSerializer extends CustomSerializer[TnAssertionReport] (format => (
  {
    case _: JValue => throw new NotImplementedError("No reason to create a TnAssertionReport object from JSON.")
  },
  {
    case tnReport : TnAssertionReport => {
      val columnsForReport = TnAssertionReportSerializer.getColumns(tnReport.query, tnReport.sampleInvalidData,
        tnReport.sampleWindowReport, tnReport.userDefinedFeatures)
      JObject(
        JField("query", JString(tnReport.query)),
        JField("description", JString(tnReport.description)),
        JField("threshold", JDouble(tnReport.threshold)),
        JField("fractionInvalid", JDouble(tnReport.fractionInvalid)),
        JField("numInvalid", JInt(tnReport.numInvalid)),
        JField("sampleInvalid", TnAssertionReportSerializer
          .dataFrameToJArray(tnReport.sampleInvalidData.select(columnsForReport.head, columnsForReport.tail: _*))),
        JField("userSummaryStatistics", (tnReport.userDefinedSummaryStats match {
          // since each summary statistic has 1 value to summarize all the rows, we only need the first row
          case Some(userDefinedSummaryStatsDF) => userDefinedSummaryStatsDF.toJSON.collect.sorted
            .map(rowJsonStr => parse(rowJsonStr)).head
          case None => JObject()
        })),
        JField("sampleWindows", (tnReport.sampleWindowReport match {
          case Some(sampleWindowReport) => JArray(sampleWindowReport.sampleWindows
            .map(df => TnAssertionReportSerializer
              .dataFrameToJArray(df.select(columnsForReport.head, columnsForReport.tail: _*))).toList)
          case None => JArray(List())
        }))
      )
    }
  }
))

object TnAssertionReportSerializer {
  val parseDriver = new ParseDriver()

  /**
    * Convert a Spark dataframe to a JsArray containing all the rows that appear in the report
    *
    * @param df The dataframe to convert
    * @return The JsArray of the dataframe
    */
  def dataFrameToJArray(df: DataFrame): JArray = JArray(df.toJSON.collect.map(rowJsonStr => parse(rowJsonStr)).toList)

  /**
    * Get the columns used in the assertion's query string as a sequence of SparkSQL columns.
    *
    * @param query The assertion's filter expression for separating valid and invalid data
    * @param sampleInvalidData A sample of the rows that are invalid according to the rule.
    * @param sampleWindowReport The windows of rows collected around each invalid row in the sample.
    * @param userDefinedFeatures The user defined features to include in sampleInvalid regardless of whether the query references them

    * @return The columns used in the assertion's query
    */
  def getColumns(query: String, sampleInvalidData: DataFrame, sampleWindowReport: Option[TnSampleWindowReport],
                 userDefinedFeatures: Seq[String]): Seq[String] = {
    // add the user defined functions to the query's columns
    // if the query is invalid, return no columns
    val queryAST =
    try {
      Some(parseDriver.parse(s"select * from testTableName where ${query}"))
    }
    catch {
      case (_: ParseException) => None
    }

    // get everything that is a table name or a column name
    def recColumnCollector(astNode: Tree): Seq[String] = {
      (0 to astNode.getChildCount).flatMap(childIdx => astNode.getChild(childIdx) match {
        case nullNode if nullNode == null => Seq[String]()
        case tableOrColNameNode if tableOrColNameNode.getType == TOK_TABLE_OR_COL => Seq(tableOrColNameNode.getChild(0).getText)
        case node => recColumnCollector(node)
      })
    }

    // in addition to columns in the query, add the user defined features and the columns for window partitioning and ordering
    // order the columns so that the partitioning and ordering columns come first
    val tableOrColumnNames =
    if (queryAST.isDefined) {
      (sampleWindowReport match {
        case Some(windowReport) => windowReport.sampleWindowParams.idsForWindowPartitioning ++ windowReport.sampleWindowParams.orderEachWindowBy
        case None => Seq()
      }) ++ recColumnCollector(queryAST.get) ++ userDefinedFeatures
    }
    else {
      Seq[String]()
    }

    // get only the columns, not the tables
    // we lowercase all values because HiveQL is case insensitive for table and column names
    sampleInvalidData.columns.map(_.toLowerCase).toSet
      .intersect(tableOrColumnNames.map(_.toLowerCase).toSet).toSeq.sorted
  }
}