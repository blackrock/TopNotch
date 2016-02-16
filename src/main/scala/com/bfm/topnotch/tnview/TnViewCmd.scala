package com.bfm.topnotch.tnview

import com.bfm.topnotch.tnengine.{Input, TnCmd}

/**
 * The class for a view command
 * @param params The object containing the parameters for the command
 * @param inputs The inputs to the View command
 */
case class TnViewCmd (
                           params: TnViewParams,
                           inputs: Seq[Input],
                           outputKey: String,
                           cache: Boolean = false,
                           outputPath: Option[String] = None
                           ) extends TnCmd

/**
 * The parameters to a view operation, independent of the input and output data.
 * @param tableAliases The names used to reference the inputs as sql tables in the query
 * @param query The HiveQL select query to combine the input tables into one table
 */
case class TnViewParams(
                         tableAliases: Seq[String],
                         query: String
                         )
