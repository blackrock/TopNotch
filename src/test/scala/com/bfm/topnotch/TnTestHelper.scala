package com.bfm.topnotch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.Matchers
import scala.io.Source
import org.json4s._
import org.json4s.native.JsonMethods._

/**
 * This class handles some of the TopNotch reusable test code
 */
object TnTestHelper extends Matchers {
  val INDEX_COL_NAME = "__INDEX_COL__"
  /**
   * Read a file from the resources/src/test/scala/com/bfm/topnotch folder
   * @param fileName The path to the file relative to the path resources/src/test/scala/com/bfm/topnotch
   * @return The contents of the file as one string
   */
  def readResourceFileToJson[T](fileName: String, classType: Class[_]): JValue = {
    parse(Source.fromFile(classType.getResource(fileName).getFile).getLines().mkString("\n"))
  }

  /**
   * Attach an index to rows into a dataframe so we can track them throughout a series of operations
   * @param df The dataframe to index
   * @return A dataframe equal to df but with an index column
   */
  def attachIdx(df: DataFrame): DataFrame = df.withColumn(INDEX_COL_NAME, monotonicallyIncreasingId()).cache

  /**
   * Get a number greater than or equal to num that is divisible by denomiator
   */
  def numDivisibleBy(num: Int, denomiator: Int) = num / denomiator * denomiator

  /**
   * Grow a data frame to a desired size by duplicating rows.
   */
  def growDataFrame(initDF: DataFrame, newSize: Int): DataFrame = {
    val initCount = initDF.count
    if (initCount < 1) throw new IllegalArgumentException("initDF's size must be greater than 0")
    List.fill((newSize / initCount + 1).toInt)(initDF).reduce(_.unionAll(_)).limit(newSize)
  }

  /**
   * Compares two dataframes and ensures that they have the same schema (ignore nullable) and the same values
   * @param actualDF The DF we want to check for correctness
   * @param correctDF The correct DF we use for comparison
   * @param onlySchema only compare the schemas of the dataframes
   */
  def dfEquals(actualDF: DataFrame, correctDF: DataFrame, onlySchema: Boolean = false): Unit = {
    actualDF.schema.map(f => (f.name, f.dataType)).toSet shouldBe correctDF.schema.map(f => (f.name, f.dataType)).toSet
    if (!onlySchema) {
      actualDF.collect.map(_.toSeq.toSet).toSet shouldBe correctDF.collect.map(_.toSeq.toSet).toSet
    }
  }
}
