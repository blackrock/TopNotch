package com.bfm.topnotch.tndiff

import com.bfm.topnotch.tnengine.{Input, TnCmd}

object TnDiffCmd {
  val DEFAULT_THRESHOLD = 1e-6
}

/**
 * The class for a diff command
 * @param params The object containing the parameters for the command
 * @param input1 The first input to the diff command
 * @param input1Name The name to use to reference the first input
 * @param input2 The second input to the diff command
 * @param input2Name The name to use to reference the second input
 * @param filterEqualRows If true, filter out rows from the diff output that are equal for all columns. This defaults to false.
 * @param threshold The default threshold to use for determining if numeric values are equal.
 */
case class TnDiffCmd (
                           params: TnDiffParams,
                           input1: Input,
                           input1Name: String,
                           input2: Input,
                           input2Name: String,
                           filterEqualRows: Option[Boolean] = None,
                           outputKey: String,
                           cache: Option[Boolean] = None,
                           threshold: Option[Double] = None,
                           outputPath: Option[String] = None,
                           tableName: Option[String] = None
                           ) extends TnCmd {
  val numericThreshold = threshold.getOrElse(TnDiffCmd.DEFAULT_THRESHOLD)
}

/**
 * The parameters to a diff operation, independent of the input and output data.
 * @param input1Columns The columns of the first input to join and diff on
 * @param input2Columns The columns of the second input to join and diff on
 * @param thresholds The threshold for which numeric values should be considered equal. This array, if set,
 *                        should have a value for every pair of columns being diffed. Otherwise TnDiffCmd's numericThreshold
 *                        will be used to compare all numeric columns.
 */
case class TnDiffParams(
                         input1Columns: TnDiffInput,
                         input2Columns: TnDiffInput,
                         thresholds: Option[Seq[Double]] = None
                         ) {
  val columnThreshold = thresholds.getOrElse(List[Double]())
}

/**
 * @param joinColumns The names of the columns to join on, in the same order as the columns they match with in the other input
 * @param diffColumns The names of the columns to diff, in the same order as the columns they match with in the other input
 */
case class TnDiffInput (
                         joinColumns: Seq[String],
                         diffColumns: Seq[String]
                         )