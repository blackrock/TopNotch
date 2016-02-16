package com.bfm.topnotch.tndiff

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}

/**
 * The class for comparing two data sets.
 */
class TnDiffCreator {
  import TnDiffCreator._

  /**
   * Produce a dataframe comparing two data sets on a row-by-row level of granularity
   * @param input1DF The first input data set to diff
   * @param input1Name The name of the first data set
   * @param input2DF The second input data set to diff
   * @param input2Name The name of the second data set
   * @param columns The TnDiffInput object for which columns to join on and diff
   * @param filterEqualRows Whether to keep rows which are equal in the two tables for the columns to diff on
   * @return A dataframe containing the diff specified by the user
   */
  def createDiff(input1DF: DataFrame, input1Name: String, input2DF: DataFrame, input2Name: String,
                 columns: TnDiffParams, numericThreshold: Double = 1e-6, filterEqualRows: Boolean = false): DataFrame = {
    validateInput(input1DF, input1Name, input2DF, input2Name, columns)

    // create new dataframes with the renamed columns
    val renamedDF1 = input1DF.select(prependColNamesWithTable(columns.input1Columns.joinColumns, input1Name, true) ++
      prependColNamesWithTable(columns.input1Columns.diffColumns, input1Name, true): _*)
    val renamedDF2 = input2DF.select(prependColNamesWithTable(columns.input2Columns.joinColumns, input2Name, true) ++
      prependColNamesWithTable(columns.input2Columns.diffColumns, input2Name, true): _*)

    // join the renamed dataframes on the columns specified by the user
    val input1RenamedJoinCols = prependColNamesWithTable(columns.input1Columns.joinColumns, input1Name)
    val input2RenamedJoinCols = prependColNamesWithTable(columns.input2Columns.joinColumns, input2Name, false)

    val joinCols = input1RenamedJoinCols
      .zip(input2RenamedJoinCols)
      .map(cols => cols._1 === cols._2)
    val joinedDF = renamedDF1.join(renamedDF2, joinCols.reduce((col1, col2) => col1 && col2), "outer")

    if (!columns.input1Columns.diffColumns.isEmpty) {
      val thresholds =
        if (columns.columnThreshold.isEmpty) Seq.fill(columns.input1Columns.diffColumns.length)(numericThreshold)
        else columns.columnThreshold
      // diff the columns specified by the user
      val diffCols =
        prependColNamesWithTable(columns.input1Columns.diffColumns, input1Name)
          .zip(prependColNamesWithTable(columns.input2Columns.diffColumns, input2Name))
          .zip(thresholds)
          .map { case ((col1, col2), e: Double) => Seq(col1, col2) ++
          diffTwoColumns(col1, joinedDF.select(col1).schema.head,
            col2, joinedDF.select(col2).schema.head, e)
        }.reduce(_ ++ _)

      val diffedDF = joinedDF.select(input1RenamedJoinCols ++ input2RenamedJoinCols ++ diffCols: _*)

      if (filterEqualRows) {
        removeEqualRowsFromDiff(input1Name, input2Name, columns, thresholds, diffedDF)
      }
      else {
        diffedDF
      }

    }
    else {
      joinedDF.select(input1RenamedJoinCols ++ input2RenamedJoinCols: _*).limit(0)
    }
  }

  /**
   * Ensure that the input to the diff function abides by its assumptions.
   */
  private def validateInput(inputView1: DataFrame, view1Name: String, inputView2: DataFrame, view2Name: String, columns: TnDiffParams): Unit = {
    if (columns.input1Columns.joinColumns.length != columns.input2Columns.joinColumns.length) {
      throw new scala.IllegalArgumentException("joinColumns must be of the same length for both data sets")
    }
    if (columns.input1Columns.diffColumns.length != columns.input2Columns.diffColumns.length) {
      throw new scala.IllegalArgumentException("diffColumns must be of the same length for both data sets")
    }
    if (columns.input1Columns.joinColumns.length == 0) {
      throw new scala.IllegalArgumentException("There must be columns to join on. joinColumns is empty for both data sets.")
    }
    if (inputView1.dropDuplicates(columns.input1Columns.joinColumns).count != inputView1.count) {
      throw new scala.IllegalArgumentException(s"The join columns for table ${view1Name} do not provide a unique key for every row.")
    }
    if (inputView2.dropDuplicates(columns.input2Columns.joinColumns).count != inputView2.count) {
      throw new scala.IllegalArgumentException(s"The join columns for table ${view2Name} do not provide a unique key for every row.")
    }
    if (inputView1.where(columns.input1Columns.joinColumns.map(inputView1(_).isNull).reduce(_ || _)).count > 0) {
      throw new scala.IllegalArgumentException(s"The join columns for table ${view1Name} contain nulls.")
    }
    if (inputView2.where(columns.input2Columns.joinColumns.map(inputView2(_).isNull).reduce(_ || _)).count > 0) {
      throw new scala.IllegalArgumentException(s"The join columns for table ${view2Name} contain nulls.")
    }
  }

  /**
   * Prepend the name of each column with that of the original table containing it. This disambiguates columns with the
   * same name that are from different inputs
   */
  private def prependColNamesWithTable(colNames: Seq[String], inputName: String, rename: Boolean = false) = {
    colNames.map(colName => {
      val newName = inputName + colJoin + colName
      if (rename) col(colName).as(newName) else col(newName)
    })
  }

  /**
   * Compare two columns for equality, check if either one is null, and take the numeric difference if they are both
   * of numeric types.
   * @param col1 The first column to compare
   * @param col1Field The type information about the first column
   * @param col2 The second column to compare
   * @param col2Field The type information about the second column
   * @return A seq of columns comparing the columns
   */
  private def diffTwoColumns(col1: Column, col1Field: StructField, col2: Column, col2Field: StructField, numericThreshold: Double): Seq[Column] = {
    //return a string summarizing the null/equality status, performing an equality check only if they are of the same type
    val nullCheck = when(col1.isNull && col2.isNull, bothNullStr)
      .when(col1.isNull, firstNullStr)
      .when(col2.isNull, secondNullStr)

    val nullEqualityCheck =
      if (col1Field.dataType != col2Field.dataType) {
        nullCheck
          .otherwise(diffTypeStr)
      } else {
        // types are the same
        (col1Field.dataType, col2Field.dataType) match {
          case (t1: NumericType, t2: NumericType) =>
            nullCheck
              .otherwise(// both not null
                when(abs(col1 - col2) <= numericThreshold, equalStr)
                  .otherwise(diffStr)
              )
          case _ =>
            nullCheck
              .when(col1.equalTo(col2), equalStr)
              .otherwise(diffStr)
        }
      }

    val nullEqualityCheckRenamed = nullEqualityCheck.as(equalityColName(col1Field.name, col2Field.name))

    (col1Field.dataType, col2Field.dataType) match {
      case (t: NumericType, _: NumericType) =>
        Seq(
          //do the diff only if both are not null, otherwise return a null cast to the type of the first data type
          when(col1.isNotNull && col2.isNotNull, when(abs(col1 - col2) >= numericThreshold, col1 - col2).otherwise(lit(0).cast(t))).otherwise(lit(null))
            .as(minusColName(col1Field.name, col2Field.name)),
          nullEqualityCheckRenamed)
      case _ => Seq(nullEqualityCheckRenamed)
    }
  }

  /**
   * Filter out the rows that are equal, within the defined thresholds
   */
  private def removeEqualRowsFromDiff(view1Name: String, view2Name: String, columns: TnDiffParams, thresholds: Seq[Double], diffedDF: DataFrame): DataFrame = {
    // keep only the rows where at least one column is different
    diffedDF.where(
      prependColNamesWithTable(columns.input1Columns.diffColumns, view1Name)
        .zip(prependColNamesWithTable(columns.input2Columns.diffColumns, view2Name))
        .zip(thresholds)
        .map {
        case ((col1, col2), e) => {
          (diffedDF.select(col1).schema.head.dataType, diffedDF.select(col2).schema.head.dataType) match {
            /* we want to collect the column iff:
                    1. both are null or
                    2. both are not null and the values differ within a threshold
                 */
            case (_: NumericType, _: NumericType) => not(col1 <=> col2) || (col1.isNotNull && col2.isNotNull && abs(col1 - col2) >= e)
            case _ => not(col1 <=> col2)
          }
        }
      }.reduce(_ || _)
    )
  }
}

/**
 * The constant strings and basic functions used to create the diff.
 */
object TnDiffCreator {
  // These are package protected so that the tests can access them.
  protected[tndiff] val equalityColValues = Seq(bothNullStr, firstNullStr, secondNullStr, diffTypeStr, equalStr, diffStr)
  protected[tndiff] val bothNullStr = "both null"
  protected[tndiff] val firstNullStr = "only first null"
  protected[tndiff] val secondNullStr = "only second null"
  protected[tndiff] val diffTypeStr = "different types"
  protected[tndiff] val equalStr = "equal"
  protected[tndiff] val diffStr = "both not null, same type, not equal"
  protected[tndiff] val colJoin = "_"

  /**
   * Get the name of the "equality column" comparing two columns. This column states whether the values are equal,
   * if one or both of the values is null, or if the values are of different types.
   * @param col1Name The name of the first column
   * @param col2Name The name of the second column
   * @return The name of the equality column
   */
  protected[tndiff] def equalityColName(col1Name: String, col2Name: String) = s"${col1Name}__equals__${col2Name}"

  /**
   * Get the name of the "minus column" comparing two numeric columns. This column states the numeric difference between
   * two columns.
   * @param col1Name The name of the first column
   * @param col2Name The name of the second column
   * @return The name of the minus column
   */
  protected[tndiff] def minusColName(col1Name: String, col2Name: String) = s"${col1Name}__minus__${col2Name}"
}
