package com.bfm.topnotch.tnassertion

import com.bfm.topnotch.tnengine.{TnInput, TnCmd}

/**
 * The class for an assertion command
 *
 * @param params The list of assertions for the command to run
 * @param input The input data to the Assertion command
 */
case class TnAssertionCmd (
                            params: TnAssertionSeq,
                            input: TnInput,
                            outputKey: String,
                            cache: Boolean = false,
                            outputPath: Option[String] = None
                                ) extends TnCmd

/**
 * This class exists because typesafe config wants a wrapper around any list of configs that it imports inside another config.
 *
 * @param assertions the assertions for the command to run.
 */
case class TnAssertionSeq(
                     assertions: Seq[TnAssertionParams]
                       )

/**
 * The parameters for an assertion operation, independent of the input and output data.
 *
 * @param query The where clause in a HiveQL query stating which rows are valid.
 * @param description A string description of what the assertion checks for
 * @param threshold The percent of invalid rows in a data set necessary to cause this assertion to fail.
 */
case class TnAssertionParams(
                              query: String,
                              description: String,
                              threshold: Double
                              )


