package com.bfm.topnotch.tnassertion

import org.antlr.runtime.tree.Tree
import org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TABLE_OR_COL
import org.apache.hadoop.hive.ql.parse.{ParseDriver, ParseException}
import org.apache.spark.sql.{Column, DataFrame}
import spray.json._


/**
  * A report on the results of checking an assertion against a data set
  *
  * @param query The rule's filter expression for separating valid and invalid data
  * @param description A description of the rule
  * @param threshold The maximum percent of invalid data allowed by the rule before it is considered a failed rule
  * @param fractionInvalid The percent data that failed the rule
  * @param numInvalid The number of rows for which the assertion was false.
  * @param sampleInvalidData A sample of the rows that are invalid according to the rule.
  * @param sampleWindowReport The report containing
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

object TNAssertionReportJsonProtocol extends DefaultJsonProtocol {

  implicit object TNReportJsonFormat extends RootJsonFormat[TnAssertionReport] {
    def write(tnReport: TnAssertionReport) = {
      val columnsForReport = getColumns(tnReport)
      JsObject(Map(
        "query" -> JsString(tnReport.query),
        "description" -> JsString(tnReport.description),
        "threshold" -> JsNumber(tnReport.threshold),
        "fractionInvalid" -> JsNumber(tnReport.fractionInvalid),
        "numInvalid" -> JsNumber(tnReport.numInvalid),
        "sampleInvalid" ->
          dataFrameToJsArray(tnReport.sampleInvalidData.select(columnsForReport.head, columnsForReport.tail: _*)),
        "userSummaryStatistics" -> (tnReport.userDefinedSummaryStats match {
          // since each summary statistic has 1 value to summarize all the rows, we only need the first row
          case Some(userDefinedSummaryStatsDF) => userDefinedSummaryStatsDF.toJSON.collect.sorted.map(_.parseJson).head
          case None => JsObject()
        }),
        "sampleWindows" -> (tnReport.sampleWindowReport match {
          case Some(sampleWindowReport) => JsArray(sampleWindowReport.sampleWindows
            .map(x => dataFrameToJsArray(x.select(columnsForReport.head, columnsForReport.tail: _*))).toVector)
          case None => JsArray()
        })
      ))
    }

    val parseDriver = new ParseDriver()

    /**
      * Convert a Spark dataframe to a JsArray containing all the rows that appear in the report
      *
      * @param df The dataframe to convert
      * @return The JsArray of the dataframe
      */
    def dataFrameToJsArray(df: DataFrame): JsArray = JsArray(df.toJSON.collect.map(_.parseJson).toVector)

    /**
      * Get the columns used in the assertion's query string as a sequence of SparkSQL columns.
      *
      * @param tnReport The report for an assertion.
      * @return The columns used in the assertion's query
      */
    def getColumns(tnReport: TnAssertionReport): Seq[String] = {
      // add the user defined functions to the query's columns
      // if the query is invalid, return no columns
      val queryAST =
        try {
          Some(parseDriver.parse(s"select * from testTableName where ${tnReport.query}"))
        }
        catch {
          case (e: ParseException) => None
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
          (tnReport.sampleWindowReport match {
            case Some(windowReport) => windowReport.sampleWindowParams.idsForWindowPartitioning ++ windowReport.sampleWindowParams.orderEachWindowBy
            case None => Seq()
          }) ++ recColumnCollector(queryAST.get) ++ tnReport.userDefinedFeatures
        }
        else {
          Seq[String]()
        }

      // get only the columns, not the tables
      // we lowercase all values because HiveQL is case insensitive for table and column names
      tnReport.sampleInvalidData.columns.map(_.toLowerCase).toSet
        .intersect(tableOrColumnNames.map(_.toLowerCase).toSet).toSeq.sorted
    }

    def read(value: JsValue) = {
      throw new NotImplementedError("No reason to create a TnAssertionReport object from JSON.")
    }
  }

}