package com.bfm.topnotch

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.{HConnection, HTableInterface, Put}
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpec
import com.typesafe.scalalogging.StrictLogging

/**
 * This class handles some of the boilerplate of testing SparkApplications with HBase writers
 */
abstract class SparkApplicationTester extends FlatSpec with MockFactory with StrictLogging with SharedSparkContext {
  protected val hconn = mock[HConnection]
  lazy val spark = SparkSession
    .builder()
    .appName(getClass.getName)
    .master("local")
    .config("spark.sql.shuffle.partitions", "4")
    //setting this to false to emulate HiveQL's case insensitivity for column names
    .config("spark.sql.caseSensitive", "false")
    .getOrCreate()

  /**
   * Verify that one the next HTable will receive the correct puts. Call this once per HTable that is supposed to be created and written to.
   * Note: All HBase tests for a SparkApplication object must be run sequentially in order for us to keep track of HTableInterface mocks
   * @param tests The test's expected name for the HTable and expected values for the Put objects placed in the HTable
   * @param acceptAnyPut Tells the mock to accept any put value. This is useful for tests using the mock and but not
   *                     testing what is put inside it.
   */
  def setHBaseMock(tests: HTableParams, acceptAnyPut: Boolean = false): Unit = {
    val table = mock[HTableInterface]
    inSequence {
      (hconn.getTable(_: String)).expects(tests.tableName).returning(table)
      inAnyOrder {
        for (correctPut <- tests.puts) {
          if (acceptAnyPut) {
            (table.put(_: Put)).expects(*)
          }
          else {
            (table.put(_: Put)).expects(where {
              (actualPut: Put) =>
                val actualValue = CellUtil.cloneValue(actualPut.get(correctPut.columnFamily, correctPut.columnQualifier).get(0))
                val eqResult = java.util.Arrays.equals(actualPut.getRow, correctPut.row) && correctPut.valueTest(actualValue)
                if (!eqResult) {
                  logger.error("A put is incorrect.")
                  logger.error("Actual value: " + correctPut.valueToString(
                    CellUtil.cloneValue(actualPut.get(correctPut.columnFamily, correctPut.columnQualifier).get(0))))
                  logger.error("Correct value: " + correctPut.valueToString(correctPut.value))
                }
                eqResult
            })
          }
        }
      }
      (table.close _).expects().returns()
    }
  }

  /**
   * The set of parameters defining what values should be used to create the HTable
   * @param tableName The name of the table the test expects to be created
   * @param puts The list of parameters for the puts that the test expects to be placed in the table
   */
  case class HTableParams(
                           tableName: String,
                           puts: Seq[HPutParams]
                           )

  /**
   * The list of values that the test expects to be in a put.
   * @param row The name of the row to put into HBase
   * @param columnFamily The cell's column family
   * @param columnQualifier The cell's column qualifier
   * @param value The value to put in the cell
   * @param valueTest The method for checking if the value in the cell is correct. Done as the actual and intended values
   *                  in a cell may be equal even if they don't have the expression as an array of bytes
   * @param valueToString A function for converting a value to a string so that incorrect values can be written to
   *                      log.
   */
  case class HPutParams(
                         row: Array[Byte],
                         columnFamily: Array[Byte],
                         columnQualifier: Array[Byte],
                         value: Array[Byte],
                         valueTest: Array[Byte] => Boolean,
                         valueToString: Array[Byte] => String
                         )
}
