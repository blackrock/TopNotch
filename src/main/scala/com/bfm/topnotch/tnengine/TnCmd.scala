package com.bfm.topnotch.tnengine

import java.io.{PrintWriter, StringWriter}

import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.writePretty

/**
 * A command for TnEngine to run
 */
abstract class TnCmd {
  /** The key to use to store the resulting dataframe in the lookup table */
  val outputKey: String
  /** Whether to cache the resulting dataframe in memory. This should be a boolean defaulting to false,
    * but json4s has a problem with default values other than None for option. Change it to a default value if json4s
    * solves the bug. */
  val cache: Option[Boolean]
  /** If writing the output to disk, the path to write to on hdfs, otherwise none */
  val outputPath: Option[String]
  /** If writing the output in hdfs, the name of the table to mount, otherwise none. Note: this will be ignored if
    * outputPath is not specified. */
  val tableName: Option[String]

  implicit val formats = Serialization.formats(NoTypeHints)
  /**
    * Overriding toString to making output of unit tests that have cmds in error logs easier to understand
    */
  override def toString = writePretty(this)
}

/**
 * The input to a command
 * @param ref The reference to the data set, either the path on hdfs or the name in the lookup table
 * @param onDisk Whether the input data set is stored on disk
 * @param delimiter The delimiter for plain text, delimited files. Leave to empty string for parquet.
 */
case class Input(ref: String, onDisk: Boolean, delimiter: Option[String] = None)

/**
 * The strings used for converting a config file into a TnCmd
 */
object TnCmdStrings {
  val ioNamespace = "io"
  val commandListStr = "commands"
  val writerStr = "writer"
  val commandStr = "command"
  val paramsStr = "params"
  val externalParamsStr = "externalParamsFile"
  val outputKeyStr = "outputKey"
  val writeToDiskStr = "writeToDisk"
  val outputPathStr = "outputPath"
}

/**
 * The class indicating that there was at least one error in the configuration for this command
 * @param cmdString The JSON string for the command.
 * @param errorStr The errors encountered in creating this command.
 * @param cmdIdx The index of the command in the plan that failed
 * @param outputKey This is meaningless in this class. This exists only so that TnErrorCmd can extend TnCmd.
 * @param writeToDisk This is meaningless in this class. This exists only so that TnErrorCmd can extend TnCmd.
 * @param outputPath This is meaningless in this class. This exists only so that TnErrorCmd can extend TnCmd.
 */
case class TnErrorCmd (
                            cmdString: String,
                            errorStr: String,
                            cmdIdx: Int,
                            outputKey: String = "",
                            cache: Option[Boolean] = None,
                            writeToDisk: Boolean = false,
                            outputPath: Option[String] = None,
                            tableName: Option[String] = None
                            ) extends TnCmd {
  override def toString: String = {
    s"There was an error with the command in position ${cmdIdx} in its plan. The command was: \n ${cmdString} \n " +
      s"The message was: \n ${errorStr} \n\n END OF ERROR MESSAGE FOR COMMAND IN POSITION ${cmdIdx} \n\n"
  }
}

object TnErrorCmd {
  /**
    * Helper method for easily getting the stack trace of an exception as a string
    * @param e The exception
    * @return The exception's stack trace
    */
  def getExceptionStackTrace(e: Exception): String = {
    val sw = new StringWriter
    e.printStackTrace(new PrintWriter(sw))
    sw.toString
  }
}