package com.bfm.topnotch.tnassertion

import com.bfm.topnotch.tnengine.{Input, TnCmd}

/**
  * The class for an assertion command
 *
  * @param params The list of assertions for the command to run
  * @param input The input data to the Assertion command
  */
case class TnAssertionCmd (
                            params: AssertionSeq,
                            input: Input,
                            outputKey: String,
                            cache: Option[Boolean] = None,
                            outputPath: Option[String] = None,
                            tableName: Option[String] = None
                          ) extends TnCmd

/**
  * This class exists because typesafe config wants a wrapper around any list of configs that it imports inside another config.
 *
  * @param assertions the assertions for the command to run.
  */
case class AssertionSeq(
                         assertions: Seq[TnAssertionParams]
                       )


/**
  * The parameters for an assertion operation, independent of the input and output data.
 *
  * @param query The where clause in a HiveQL query stating which rows are valid.
  * @param description A string description of what the assertion checks for
  * @param threshold The percent of invalid rows in a data set necessary to cause this assertion to fail.
  * @param userDefinedSummaryExpr Expressions to produce summary statistics of the invalid data points defined by the user for the tnassertion report
  * @param userDefinedFeatures Additional features defined by the user to be added to each row of the data set
  * @param sampleWindowParams The parameters for adding ordered windows of data containing each invalid point to an assertion report.
  */
case class TnAssertionParams(
                              query: String,
                              description: String,
                              threshold: Double,
                              userDefinedSummaryExpr: Option[Map[String, String]] = None,
                              userDefinedFeatures: Option[Map[String, String]] = None,
                              sampleWindowParams: Option[TnSampleWindowParams] = None
                            )

/**
  * The parameters for adding ordered windows of data containing each invalid point to an assertion report.
  * @param idsForWindowPartitioning The columns to use as ids for partitioning the data set into windows.
  * @param orderEachWindowBy The columns to use for ordering each window.
  */
case class TnSampleWindowParams(
                                 idsForWindowPartitioning: Seq[String],
                                 orderEachWindowBy: Seq[String]
                              )