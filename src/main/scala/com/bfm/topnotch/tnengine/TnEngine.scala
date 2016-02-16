package com.bfm.topnotch.tnengine

import com.bfm.topnotch.tnengine.TnCmdStrings._
import com.databricks.spark.csv.CsvParser
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkContext, SparkConf, Logging}
import com.typesafe.config.Config
import com.bfm.topnotch.tnassertion.{TnAssertionCmd, TnAssertionRunner}
import com.bfm.topnotch.tndiff.{TnDiffCmd, TnDiffCreator}
import com.bfm.topnotch.tnview.{TnViewCmd, TnViewCreator}
import org.apache.spark.sql.{SaveMode, DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import scala.collection.mutable.{Map => MMap, Set => MSet}
import scala.collection.JavaConversions.asScalaBuffer

/**
 * The entry for Spark into TopNotch.
 */
object TnEngine extends Logging {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    if (!conf.contains("spark.master")) {
      conf.setMaster("local[2]")
    }
    if (!conf.contains("spark.app.name")) {
      conf.setAppName(getClass.getName)
    }
    //There should only be 1 argument, the path to the plan config file
    if (args.length != 1) {
      throw new IllegalArgumentException("TopNotch accepts exactly 1 argument: the name of the plan config file. \n" +
        s"The arguments were: ${args.reduce(_ + " , " + _)}")
    }

    new TnEngine(new HiveContext(new SparkContext(conf))).run(args(0))
  }
}

/**
 * The class for running a plan.
 * @param sqlContext The context used to access the Spark cluster
 */
class TnEngine(sqlContext: SQLContext) extends Logging {

  // A lookup table where the dataframe outputs of commands are stored so that later commands can access them
  val dataframeLookupTable: MMap[String, DataFrame] = MMap.empty

  /**
   * Parse a plan file and then run the desired commands
   * @param configFile The name of the plan file to load
   */
  def run(configFile: String): Unit = {
    log.info("parsing configurations")

    val rootConfig = ConfigFactory.load(configFile)
    val cmds = parseCommands(rootConfig)

    val errorsStr = collectErrors(cmds)
    if (errorsStr.isDefined) {
      throw new IllegalArgumentException(errorsStr.get)
    }

    log.info("parsing successful, running commands")
    val persister = getPersister(rootConfig)
    executeCommands(cmds, new TnAssertionRunner(persister), new TnDiffCreator(), new TnViewCreator(sqlContext))
  }

  /**
   * Collect all the TnErrorCmds in a TnCmd sequence into one error string
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
   * @param input The input to get
   * @return The input data set as a dataframe
   */
  protected[tnengine] def getInputDF(input: Input): DataFrame = {
    //errors with variable definition should have already been caught in parseConfig, so not looking hard here.
    if (input.onDisk) {
      //look for delimiter, If none is provided , treat the input file as parquet
      val df = input.delimiter match {
        case del if del == "" => sqlContext.read.parquet(input.ref)
        case del => new CsvParser()
          .withUseHeader(true)
          .withInferSchema(true)
          .withDelimiter(del.charAt(0))
          .csvFile(sqlContext, input.ref)
      }
      dataframeLookupTable.put(input.ref, df)
    }
    dataframeLookupTable.get(input.ref) match {
      case Some(retDF) => retDF
      case None => throw new IllegalArgumentException(s"Input ${input.ref} not in lookup table.")
    }
  }

  /**
   * Execute a sequence of commands
   * @param cmds The commands to execute
   * @param assertionRunner The instance of TnAssertionRunner to use to run the assertion commands
   * @param diffCreator The instance of TnDiffCreator to use to run the diff commands
   * @param viewCreator The instance of TnViewCreator to use to run the view commands
   */
  protected[tnengine] def executeCommands(cmds: Seq[TnCmd], assertionRunner: TnAssertionRunner,
                                          diffCreator: TnDiffCreator, viewCreator: TnViewCreator): Unit = {

    /**
     * Store the output dataframe in the lookup table or on disk as specified in the command
     * @param output The output to store
     * @param cmd The command that generated it
     */
    def storeOutputDF(output: DataFrame, cmd: TnCmd): Unit = {
      dataframeLookupTable.put(cmd.outputKey, if (cmd.cache) {output.cache()} else output)
      if (cmd.outputPath.isDefined) {
        // ok to just get the dataframe and trust that it is there because I just put it in the map
        dataframeLookupTable.get(cmd.outputKey).get.write.mode(SaveMode.Overwrite).format("parquet").save(cmd.outputPath.get)
      }
    }
    
    // execute the commands
    cmds.foreach(cmd => storeOutputDF(
      cmd match {
      case assertionCmd: TnAssertionCmd => assertionRunner.runAssertions(getInputDF(assertionCmd.input),
        assertionCmd.outputKey, assertionCmd.params.assertions)
      case diffCmd: TnDiffCmd => diffCreator.createDiff(getInputDF(diffCmd.input1), diffCmd.input1Name,
        getInputDF(diffCmd.input2), diffCmd.input2Name, diffCmd.params, diffCmd.numericThreshold, diffCmd.filterEqualRows)
      case viewCmd: TnViewCmd => viewCreator.createView(viewCmd.inputs.map(getInputDF), viewCmd.params)
    }, cmd))
  }

  /**
   * Get the appropriate persister object depending on the configuration file
   * @param rootConfig The root of the config file
   */
  protected[tnengine] def getPersister(rootConfig: Config): TnPersister = {
    lazy val defaultResult = new TnHDFSPersister()
    if (rootConfig.hasPath(tnIONamespace)) {
      val ioConfig = rootConfig.getConfig(tnIONamespace)
      if (ioConfig.hasPath(persisterStr)) {
        ioConfig.as[String](persisterStr) match {
          case "hbase" => new TnHBasePersister
          case "hdfs" => rootConfig.as[TnHDFSPersister](tnIONamespace)
          }
      }
      else defaultResult
    }
    else defaultResult
  }

  /**
   * Parse the given config file for commands and report all valid or invalid commands
   * @param rootConfig The root of the config file
   * @return The list of commands, either valid commands to run or errors of incorrectly specified commands
   */
  protected[tnengine] def parseCommands(rootConfig: Config): Seq[TnCmd] = {
    import TnCmdStrings._

    // a running list of the output keys used so far
    val definedOutputKeys: MSet[String] = MSet.empty
    val fs = FileSystem.get(sqlContext.sparkContext.hadoopConfiguration)
     /**
     * Determine if all inputs to each command are valid: either having been previously defined in this run or can be
     * loaded from disk. If they are valid, add the outputs of the command to the set of valid output keys. Throw
     * an exception if any inputs are invalid.
     *
     * @param inputs The input refs to check
     * @param cmd The command which uses the input refs
     */
    def inputValidityCheck(inputs: Seq[Input], cmd: TnCmd): Unit = {
      val invalidList = inputs.map(v => ((definedOutputKeys.contains(v.ref) || (fs.exists(new Path(v.ref)) && v.onDisk)), v.ref))
        .filter(_._1 == false)
      if (invalidList.isEmpty) {
        // add the output key reference as that is now valid
        definedOutputKeys.add(cmd.outputKey)
      }
      else {
        throw new IllegalArgumentException(s"The following input refs are invalid: ${invalidList.map(_._2).reduce(_ + ", " + _)}")
      }
    }

    //tell the user if the topnotch config namespace doesn't exist
    if (!rootConfig.hasPath(tnNamespace)) {
      throw new IllegalArgumentException("The plan doesn't have a topnotch namespace. All commands must be contained within " +
        "the topnotch namespace. \n\n The entire loaded config is: \n\n " + rootConfig.toString)
    }

    // TODO: figure out why Intellij doesn't like this without explicit type
    val configsWithIndex: List[(Config, Int)] = rootConfig.getConfigList(tnNamespace).toBuffer.toList.zipWithIndex
    configsWithIndex.map { case (cmdConfig, i) => {
      try {
        // I wrap this all in another namespace because the as command must be called one namespace level up from the
        // one it is converting to a case class
        val mergedWithExternalParams = ConfigFactory.empty().withValue(wrapper,
            cmdConfig.withValue(paramsStr, ConfigFactory.load(cmdConfig.getString(externalParamsStr)).getObject(tnNamespace))
              .root()
        )
        cmdConfig.as[String](commandStr) match {
          case "assertion" => {
            val cmd = mergedWithExternalParams.as[TnAssertionCmd](wrapper)
            inputValidityCheck(Seq(cmd.input), cmd)
            cmd
          }
          case "diff" => {
            val cmd = mergedWithExternalParams.as[TnDiffCmd](wrapper)
            inputValidityCheck(Seq(cmd.input1, cmd.input2), cmd)
            cmd
          }
          case "view" => {
            val cmd = mergedWithExternalParams.as[TnViewCmd](wrapper)
            inputValidityCheck(cmd.inputs, cmd)
            cmd
          }
          case badValue => throw new IllegalArgumentException(s"The value for $paramsStr, ${badValue}, is invalid. " +
            s"It must be diff, assertion, or view.")
        }
      }
      catch {
        case e: Throwable =>
          TnErrorCmd(e.getMessage, i)
      }
    }}
  }
}