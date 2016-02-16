package com.bfm.topnotch.tnassertion

import org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TABLE_OR_COL
import org.antlr.runtime.tree.Tree
import org.apache.hadoop.hive.ql.parse.{ParseException, ParseDriver}
import org.apache.spark.Logging
import org.apache.spark.sql.{Column, DataFrame}
import spray.json._

/**
 * A report on the results of checking an assertion against a data set
 * @param query The rule's filter expression for separating good and bad data
 * @param description A description of the rule
 * @param threshold The maximum percent of bad data allowed by the rule before it is considered a failed rule
 * @param fractionBad The percent data that failed the rule
 * @param sampleBadData A sample of the rows that are bad according to the rule.
 */
case class TnAssertionReport(
                              query: String,
                              description: String,
                              threshold: Double,
                              fractionBad: Double,
                              sampleBadData: DataFrame
                              )

object TNAssertionReportJsonProtocol extends DefaultJsonProtocol {

  implicit object TNReportJsonFormat extends RootJsonFormat[TnAssertionReport] {
    def write(tnReport: TnAssertionReport) = JsObject(
      "Query" -> JsString(tnReport.query),
      "Description" -> JsString(tnReport.description),
      "Threshold" -> JsNumber(tnReport.threshold),
      "FractionBad" -> JsNumber(tnReport.fractionBad),
      "SampleBad" -> JsArray(tnReport.sampleBadData.select(getColumns(tnReport): _*).toJSON.collect.sorted.map(_.parseJson).toVector)
    )


    val parseDriver = new ParseDriver()

    /**
     * Get the columns used in the assertion's query string as a sequence of SparkSQL columns.
     * @param tnReport The report for an assertion.
     * @return The columns used in the assertion's query
     */
    def getColumns(tnReport: TnAssertionReport): Seq[Column] = {
      // if the query is invalid, return no columns
      val queryAST =
        try {
          Some(parseDriver.parse(s"select * from testTableName where ${tnReport.query}"))
        }
        catch {
          case (e: ParseException) => None
        }

      //get everything that is a table name or a column name
      def recColumnCollector(astNode: Tree): Seq[String] = {
        (0 to astNode.getChildCount).flatMap(childIdx => astNode.getChild(childIdx) match {
          case nullNode if nullNode == null => Seq[String]()
          case tableOrColNameNode if tableOrColNameNode.getType == TOK_TABLE_OR_COL => Seq(tableOrColNameNode.getChild(0).getText)
          case node => recColumnCollector(node)
        })
      }
      val tableOrColumnNames = if (queryAST.isDefined) recColumnCollector(queryAST.get) else Seq[String]()

      //get only the columns, not the tables
      //we lowercase all values because HiveQL is case insensitive for table and column names
      tnReport.sampleBadData.columns.map(_.toLowerCase()).toSet
      val result = tnReport.sampleBadData.columns.toSet.map((s: String) => s.toLowerCase)
        .intersect(tableOrColumnNames.toSet.map((s: String) => s.toLowerCase))
        .map(tnReport.sampleBadData(_)).toSeq
      result
    }

    def read(value: JsValue) = {
      throw new NotImplementedError("No reason to create a TnAssertionReport object from JSON.")
    }
  }

}