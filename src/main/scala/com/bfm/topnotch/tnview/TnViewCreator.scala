package com.bfm.topnotch.tnview

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession


/**
 * The class for combining multiple data sets into one that can be used as an input to the diff and assertion commands.
 * This one data set is a "view" of the many used to create it.
 * @param spark The SparkSession to use for creating views
 */
class TnViewCreator(spark: SparkSession) {

  /**
   * Create a view using from multiple data sets using a sql statement
   * @param inputs The inputs to create views from
   * @param params The HiveQL statement used to create the new view and the input tables' names in the statement
   * @return The new view in the form of a dataframe
   */
  def createView(inputs: Seq[DataFrame], params: TnViewParams): DataFrame = {
    // register the views as temporary tables accessible from sql queries
    inputs.zip(params.tableAliases).foreach{
      case (view, name) => view.createOrReplaceTempView(name)
    }
    spark.sql(params.query)
  }
}
