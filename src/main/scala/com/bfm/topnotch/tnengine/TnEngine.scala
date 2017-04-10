package com.bfm.topnotch.tnengine

import java.io.{File, PrintWriter, StringWriter}
import java.net.URL

import com.bfm.topnotch.tnassertion.{TnAssertionCmd, TnAssertionRunner}
import com.bfm.topnotch.tndiff.{TnDiffCmd, TnDiffCreator}
import com.bfm.topnotch.tnengine.TnCmdStrings._
import com.bfm.topnotch.tnengine.TnEngine.TnCLIConfig
import com.bfm.topnotch.tnview.{TnViewCmd, TnViewCreator}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.Extraction.decompose
import org.json4s.native.Serialization
import org.json4s.native.Serialization.writePretty
import org.apache.log4j.{Level, Logger}
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.FilenameUtils

import scala.collection.mutable.{Map => MMap, Set => MSet}

/**
  * The entry for Spark into TopNotch.
  */
object TnEngine extends StrictLogging {

  def main(args: Array[String]): Unit = {
    // set root logging to error but maintain topnotch logging
    Logger.getRootLogger.setLevel(Level.ERROR)
    Logger.getLogger("com.bfm.topnotch").setLevel(Level.INFO)

    val spark = SparkSession
      .builder()
      .appName("TopNotch")
      .enableHiveSupport()
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.speculation", true)
      .getOrCreate()

    parser.parse(args, TnCLIConfig()) match {
      // Only need to worry about success case as parser will show error message
      case Some(config) => {
        val failedAssertions = new TnEngine(spark).run(config)
        logger.info("Number of failed assertions: " + failedAssertions.toString())
        System.exit(if (failedAssertions > 0) ASSERTIONS_FAILED_EXIT_CODE else SUCCESS_EXIT_CODE)
      }
      case None =>
        System.exit(INVALID_ARGUMENTS_EXIT_CODE)
    }
  }

  case class TnCLIConfig(planPath: String = "", planServerURL: Option[String] = None, reportKey: Option[String] = None,
                         haltIfPotentialErrors: Boolean = false, variableDictionary: Map[String, String] = Map.empty)

  val parser = new scopt.OptionParser[TnCLIConfig]("scopt") {
    head("scopt", "3.x")

    note("If the plan and commands are standalone files on disk, set planPath to the path on disk.\n"+
      "If the plan and commands are packaged in one or more jars, add the jars containing the plans and commands to " +
      "the Spark submit --jars flag. (Note: TopNotchRunner.sh does not support the --jars flag) Then, set --planPath " +
      "to the classpath to the jar.\n" +
      "If the plan and commands are accessible by a REST API, set planServer to the base URL for the API and planPath " +
      "to the route in the API for accessing the plan.")

    opt[String]('l', "planPath").required().valueName("<path>").action( (x, c) =>
      c.copy(planPath = x) ).text("planPath is the path to the plan on disk or relative to planServerURL")

    opt[String]('s', "planServerURL").valueName("<URL>").action( (x, c) =>
      c.copy(planServerURL = Some(x)) ).text("planServerURL is the base URL of the REST server for loading a plan. " +
      "Note that this URL should not include the route relative to the URL for loading the plan")

    opt[String]('k', "reportKey").valueName("<key>").action( (x, c) =>
      c.copy(planPath = x) ).text("the key for referring to the report. " +
      "The filename from planPath will be used if this is not set")

    opt[Unit]('c', "haltIfPotentialErrors").action( (_, c) =>
      c.copy(haltIfPotentialErrors = true) ).text("If set, TopNotch will halt after parsing and before before executing any commands " +
      "if there are potential errors in the configurations. By default, TopNotch will parse the configs, print a warning," +
      "and proceed with execution in the event of potential errors as certain valid configurations may appear to be invalid.")

    opt[Map[String,String]]('d', "variableDictionary").valueName("variable1=value1,variable2=value2...").action( (x, c) =>
      c.copy(variableDictionary = x) ).text("variables and values for string replacement in the plan and commands")
  }

  //When a non-assertion command is run, there are no additional failures of assertions.
  val NO_FAILURES = 0

  val ASSERTIONS_FAILED_EXIT_CODE = 3
  val INVALID_ARGUMENTS_EXIT_CODE = 4
  val SUCCESS_EXIT_CODE = 0
}

/**
  * The class for running a plan.
  *
  * @param spark The SparkSession used to access the Spark cluster
  */
class TnEngine(spark: SparkSession) extends StrictLogging {

  implicit val formats = Serialization.formats(NoTypeHints)

  // A lookup table where the dataframe outputs of commands are stored so that later commands can access them
  val dataframeLookupTable: MMap[String, DataFrame] = MMap.empty

  /**
    * Parse a plan file and then run the desired commands
    *
    * @param args The arguments for the program.
    * @return Returns the number of assertions that failed (had an error percentage greater than the threshold)
    */
  def run(args: TnCLIConfig): Int = {
    logger.info("parsing plan and commands")
    logger.info("TnEngine Runner arguments: " + args.toString)
    val planLocation = args.planPath

    val reader = args.planServerURL match {
      case Some(url) =>  new TnRESTReader(new URL(url), args.variableDictionary)
      case None => new TnFileReader(args.variableDictionary)
    }

    val rootAST = reader.readConfiguration(planLocation)
    logger.info(rootAST.toString)
    val writer = getWriter(rootAST)
    val cmds = parseCommands(rootAST, reader)

    val errorsStr = collectErrors(cmds)
    if (errorsStr.isDefined) {
      val errorString = "Possible error parsing plans and commands.\n" + errorsStr.get
      if (args.haltIfPotentialErrors) {
        throw new IllegalArgumentException(errorString)
      }
      else {
        logger.error(errorString + "\nContinuing with execution despite possible issue with plans and commands. " +
          s"Running commands: \n${writePretty(cmds)}")
      }
    }
    else {
      logger.info(s"parsing successful, running commands: \n${writePretty(cmds)}")
    }


    executeCommands(cmds, args.reportKey.getOrElse(FilenameUtils.getName(args.planPath)), writer,
      new TnAssertionRunner(writer),
      new TnDiffCreator(),
      new TnViewCreator(spark))
  }

  /**
    * Collect all the TnErrorCmds in a TnCmd sequence into one error string
    *
    * @param cmds The commands to check for errors
    * @return None if there are no errors or the merged string to throw if there are errors.
    */
  protected[tnengine] def collectErrors(cmds: Seq[TnCmd]): Option[String] = {
    //if there are any errors, append them all and throw them
    val errors = cmds.collect({ case e: TnErrorCmd => e })
    if (!errors.isEmpty) {
      return Some(errors.map(_.toString).reduce(_ + _))
    }
    else {
      None
    }
  }

  /**
    * Get the input data, either from the lookup table or from disk
    *
    * @param input The input to get
    * @return The input data set as a dataframe
    */
  protected[tnengine] def getInputDF(input: Input): DataFrame = {
    //errors with variable definition should have already been caught in parseCommands, so not looking for every issue here.
    if (input.onDisk) {
      //look for delimiter, If none is provided , treat the input file as parquet
      val df = input.delimiter match {
        case None => spark.read.parquet(input.ref)
        case Some(del) => spark.read.format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .option("delimiter", del.substring(0, 1))
          .load(input.ref)
      }
      dataframeLookupTable.put(input.ref, df)
    }
    dataframeLookupTable.get(input.ref) match {
      case Some(retDF) => retDF
      case None => throw new IllegalArgumentException(s"Input ${input.ref} not in lookup table.")
    }
  }

  /**
    * Execute a sequence of commands and write the output
    *
    * @param cmds            The commands to execute
    * @param reportKey       The key used to reference the report when writing the results
    * @param writer          The writer for writing the reports to a storage location
    * @param assertionRunner The instance of TnAssertionRunner to use to run the assertion commands
    * @param diffCreator     The instance of TnDiffCreator to use to run the diff commands
    * @param viewCreator     The instance of TnViewCreator to use to run the view commands
    * @return The number of assertion commands that failed
    */
  protected[tnengine] def executeCommands(cmds: Seq[TnCmd], reportKey: String, writer: TnWriter,
                                          assertionRunner: TnAssertionRunner,
                                          diffCreator: TnDiffCreator, viewCreator: TnViewCreator): Int = {

    /**
      * Store the output dataframe in the lookup table or on disk as specified in the command
      *
      * @param output The output to store
      * @param cmd    The command that generated it
      */
    def storeOutputDF(output: DataFrame, cmd: TnCmd): Unit = {
      logger.info(s"Storing ${cmd.outputKey} in lookup table.")
      dataframeLookupTable.put(cmd.outputKey, if (cmd.cache.getOrElse(false)) output.cache() else output)
      if (cmd.outputPath.isDefined) {
        // ok to just get the dataframe and trust that it is there because I just put it in the map
        logger.info(s"Attempting to write ${cmd.outputKey} to disk in location ${cmd.outputPath.get}.")
        dataframeLookupTable.get(cmd.outputKey).get.write.mode(SaveMode.Overwrite).format("parquet").save(cmd.outputPath.get)
        logger.info(s"Successfully wrote ${cmd.outputKey} to disk in location ${cmd.outputPath.get}.")
        if (cmd.tableName.isDefined) {
          logger.info(s"Attempting to mount ${cmd.outputKey} as a table with name ${cmd.tableName.get}.")
          spark.catalog.createExternalTable(cmd.tableName.get, cmd.outputPath.get, source = "parquet")
          logger.info(s"Successfully mounted ${cmd.outputKey} as a table with name ${cmd.tableName.get}.")

        }
      }
    }

    /**
      * Execute an individual command by using a match to figure out which command and store the output DataFrame.
      * @param cmd An input command passed in from running executeCommands.
      * @return Appropriate assertion exit code (i.e. 1 assertion command failed 0 else or if not assertion)
      */
    def runCommand(cmd: TnCmd): Int = {
      cmd match {
        case assertionCmd: TnAssertionCmd => {
          logger.info(s"Executing assertion: \n ${writePretty(assertionCmd)}")
          val out = assertionRunner.runAssertions(getInputDF(assertionCmd.input),
            assertionCmd.outputKey, assertionCmd.params.assertions)
          storeOutputDF(out.df, cmd)
          out.numFailed
        }
        case diffCmd: TnDiffCmd => {
          logger.info(s"Executing diff: \n ${writePretty(diffCmd)}")
          storeOutputDF(diffCreator.createDiff(getInputDF(diffCmd.input1), diffCmd.input1Name,
            getInputDF(diffCmd.input2), diffCmd.input2Name, diffCmd.params, diffCmd.numericThreshold,
            diffCmd.filterEqualRows.getOrElse(false)), cmd)
          TnEngine.NO_FAILURES
        }
        case viewCmd: TnViewCmd => {
          logger.info(s"Executing view: \n ${writePretty(viewCmd)}")
          storeOutputDF(viewCreator.createView(viewCmd.inputs.map(getInputDF), viewCmd.params), cmd)
          TnEngine.NO_FAILURES
        }
      }
    }

    writer.addSectionToReport(decompose(cmds))
    val numFailures = cmds.foldLeft(0)((last: Int, cmd: TnCmd) => last + runCommand(cmd))
    writer.writeReport(reportKey)
    numFailures
  }


  /**
    * Get the appropriate writer object depending on the value specified in the plan file
    *
    * @param rootAST The root of the JSON AST of the plan
    */
  protected[tnengine] def getWriter(rootAST: JValue): TnWriter = {
    val writerAST = rootAST \ ioNamespace \ writerStr
    //default to using hdfs
    if (writerAST != JNothing) {
      writerAST.extract[String] match {
        case TnWriterConfigStrings.HBASE_CONF_STRING => new TnHBaseWriter
        case TnWriterConfigStrings.HDFS_CONF_STRING => (rootAST \ ioNamespace).extract[TnHDFSWriter]
        case TnWriterConfigStrings.REST_CONF_STRING => (rootAST \ ioNamespace).extract[TnRESTWriter]
      }
    }
    else new TnHDFSWriter()
  }

  /**
    * Parse the given AST file for commands and report all valid or invalid commands
    *
    * @param rootAST The root of the AST of the plan file
    * @param reader The instance of TnReader for reading plans and commands
    * @param definedOutputKeys The set of defined output keys used so far. Empty by default. Set this for recursive calls
    *                          to allow a nested plan access to its parent's set of defined output keys.
    * @return The list of commands, either valid commands to run or errors of incorrectly specified commands
    */
  protected[tnengine] def parseCommands(rootAST: JValue, reader: TnReader,
                                        definedOutputKeys: MSet[String] = MSet.empty): Seq[TnCmd] = {
    import TnCmdStrings._

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    /**
      * Determine if all inputs to each command are valid: either having been previously defined in this run or can be
      * loaded from disk. If they are valid, add the outputs of the command to the set of valid output keys. Throw
      * an exception if any inputs are invalid.
      *
      * @param inputs The input refs to check
      * @param cmd    The command which uses the input refs
      *
      * @return cmd if command is valid,
      */
    def inputValidityCheck(inputs: Seq[Input], cmd: TnCmd, cmdStr: JValue, commandIndex: Int): TnCmd = {
      val invalidList = inputs.map(v => ((definedOutputKeys.contains(v.ref) && !v.onDisk) || (fs.exists(new Path(v.ref)) && v.onDisk), v.ref))
        .filter(_._1 == false)
      // add the output key reference as valid
      definedOutputKeys.add(cmd.outputKey)
      if (!invalidList.isEmpty) {
        TnErrorCmd(writePretty(cmdStr),
          s"The following input refs are invalid: ${invalidList.map(_._2).reduce(_ + ", " + _)}", commandIndex)
      }
      else {
        cmd
      }
    }

    logger.info("Starting loop")
    (rootAST \ commandListStr).children.zipWithIndex.flatMap { case (cmdAST, i) => {
      logger.info(s"Stepping $i")
      try {
        val mergedWithExternalParams = cmdAST merge JObject(JField(paramsStr,
          reader.readConfiguration((cmdAST \ externalParamsStr).extract[String], Some(rootAST))
        ))
        (cmdAST \ commandStr).extract[String] match {
          case "assertion" => {
            val cmd = mergedWithExternalParams.extract[TnAssertionCmd]
            Seq(inputValidityCheck(Seq(cmd.input), cmd, mergedWithExternalParams, i))
          }
          case "diff" => {
            val cmd = mergedWithExternalParams.extract[TnDiffCmd]
            Seq(inputValidityCheck(Seq(cmd.input1, cmd.input2), cmd, mergedWithExternalParams, i))
          }
          case "view" => {
            val cmd = mergedWithExternalParams.extract[TnViewCmd]
            Seq(inputValidityCheck(cmd.inputs, cmd, mergedWithExternalParams, i))
          }
          case "plan" => {
            parseCommands(mergedWithExternalParams \ paramsStr, reader, definedOutputKeys)
          }
          case invalidValue => Seq(TnErrorCmd(writePretty(cmdAST), s"The value ${invalidValue}, " +
            s"which is given with key $commandStr to specify the command type, is invalid. " +
            s"It must be diff, assertion, or view.", i))
        }
      }
      catch {
        case e: Exception => {
          Seq(TnErrorCmd(pretty(render(cmdAST)), TnErrorCmd.getExceptionStackTrace(e), i))
        }
      }
    }
    }
  }
}