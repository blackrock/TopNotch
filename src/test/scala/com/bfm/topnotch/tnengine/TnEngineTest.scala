package com.bfm.topnotch.tnengine

import java.io.File

import com.bfm.topnotch.{SparkApplicationTester, tnengine}
import com.bfm.topnotch.tnassertion.{AssertionSeq, TnAssertionCmd, TnAssertionParams, TnAssertionRunner}
import com.bfm.topnotch.tndiff.{TnDiffCmd, TnDiffCreator, TnDiffInput, TnDiffParams}
import com.bfm.topnotch.tnengine.TnEngine.{TnCLIConfig, NO_FAILURES, ASSERTIONS_FAILED_EXIT_CODE}
import com.bfm.topnotch.tnview.{TnViewCmd, TnViewCreator, TnViewParams}
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{FalseFileFilter, TrueFileFilter}
import org.scalatest.{Matchers, Tag}

/**
 * The tests for [[com.bfm.topnotch.tnengine.TnEngine TnEngine]].
 */
class TnEngineTest extends SparkApplicationTester with Matchers {

  lazy val fileReader = new TnFileReader
  lazy val engine = new TnEngine(spark)
  lazy val diffCreator = new TnDiffCreator()
  lazy val assertionRunner = new TnAssertionRunner(new TnHBaseWriter(Some(hconn)))
  lazy val viewCreator = new TnViewCreator(spark)
  val outputDir = new File(getClass.getResource("testOutput").getFile)

  /**
   * The tags
   */
  object tnEngineTag extends Tag("TnEngine")
  object collectErrorsTag extends Tag("collectErrors")
  object getConfigTag extends Tag("getConfig")
  object executeCommandsTags extends Tag("executeCommands")
  object parseConfigTag extends Tag("parseConfig")
  object getInputDFTag extends Tag("getInputDF")
  object cacheTag extends Tag("cacheOutput")
  object splitArgsTag extends Tag("splitArgs")
  object runTag extends Tag("run")


  /**
   * The plans
   */
  //this plan is complete, just 3 commands, but tests all features and should be the same as test/resources/com/bfm/topnotch/tnengine/allCmdsPlan.json
  val allCmdsPlan = Seq[TnCmd](
    TnViewCmd(
      TnViewParams(Seq("loanData"), "select * from loanData"),
      Seq(Input("src/test/resources/com/bfm/topnotch/tnview/currentLoans.parquet", true)),
      "viewKey",
      Some(true)
    ),
    TnDiffCmd(
      TnDiffParams(
        TnDiffInput(Seq("loanID", "poolNum"), Seq("loanBal")),
        TnDiffInput(Seq("loanIDOld", "poolNumOld"), Seq("loanBalOld"))
      ),
      Input("src/test/resources/com/bfm/topnotch/tndiff/currentLoans.parquet", true),
      "cur",
      Input("src/test/resources/com/bfm/topnotch/tndiff/oldLoans.parquet", true),
      "old",
      None,
      "diffKey",
      outputPath = Some("target/test-classes/com/bfm/topnotch/tnengine/testOutput/diffOutput.parquet")
    ),
    TnAssertionCmd(
      AssertionSeq(
        Seq(
          TnAssertionParams("loanBal > 0", "Loan balances are positive", 0.01),
          TnAssertionParams("loanBal > 1", "Loan balances are greater than 1", 0.02)
        )
      ),
      Input("viewKey", false),
      "assertionKey",
      None,
      Some("target/test-classes/com/bfm/topnotch/tnengine/testOutput/assertionOutput.parquet")
    )
  )

  //this plan has 2 errors and 1 correct command
  val (firstError, secondError) = (TnErrorCmd("erroneous command 1", "first error", 0), TnErrorCmd("erroneous command 2", "second error", 2))
  val errorPlan = Seq[TnCmd](firstError, allCmdsPlan(0), secondError, allCmdsPlan(0))
  //this plan has one command that is correct. It should be the same as test/resources/com/bfm/topnotch/tnengine/oneCorrectPlan.json
  val oneCorrectPlan = allCmdsPlan.slice(0, 1)

  /**
   * Verify that a seq of cmd objects has N errors.
    *
    * @param cmds the list of parsed commands
   * @param numErrors the number of errors to have
   * @param numNotErrors the number of not erroneous commands to have, default is all are errors
   */
  def shouldContainNErrors(cmds: Seq[TnCmd], numErrors: Int, numNotErrors: Int = 0): Unit = {
    cmds.filter(_.isInstanceOf[TnErrorCmd]) should have length numErrors
    cmds should have length (numErrors + numNotErrors)
  }

  "collectErrors" should "return None when given an empty list of commands" taggedAs(tnEngineTag, collectErrorsTag) in {
    engine.collectErrors(Seq()) shouldBe None
  }

  it should "return None when given a list of commands with no errors" taggedAs(tnEngineTag, collectErrorsTag) in {
    engine.collectErrors(allCmdsPlan) shouldBe None
  }

  it should "return only the errors as one string when given a list of commands with multiple errors " +
    "and correct commnds" taggedAs(tnEngineTag, collectErrorsTag) in {
    engine.collectErrors(errorPlan) shouldBe Some(firstError.toString + secondError.toString)
  }

  "getConfig" should "throw an IllegalArgumentException when the plan doesn't exist" taggedAs(tnEngineTag, getConfigTag) in {
    intercept[IllegalArgumentException] {
      (new TnFileReader).readConfiguration("src/test/resources/com/bfm/DOESNTEXIST")
    }
  }

  "parseCommands" should "return None when the plan is empty" taggedAs(tnEngineTag, parseConfigTag) in {
    engine.parseCommands(fileReader.readConfiguration("src/test/resources/com/bfm/topnotch/tnengine/emptyPlan.json"), fileReader) shouldBe Seq()
  }

  it should "return an error when referencing a nonexistant command" taggedAs(tnEngineTag, parseConfigTag) in {
    shouldContainNErrors(engine.parseCommands(fileReader.readConfiguration("src/test/resources/com/bfm/topnotch/tnengine/noCmdPlan.json"), fileReader), 1)
  }

  it should "return an error when referencing a nonexistant file" taggedAs(tnEngineTag, parseConfigTag) in {
    shouldContainNErrors(engine.parseCommands(fileReader.readConfiguration("src/test/resources/com/bfm/topnotch/tnengine/noFilePlan.json"), fileReader), 1)
  }

  it should "return an error when referencing a nonexistant variable" taggedAs(tnEngineTag, parseConfigTag) in {
    shouldContainNErrors(engine.parseCommands(fileReader.readConfiguration("src/test/resources/com/bfm/topnotch/tnengine/noVarPlan.json"), fileReader), 1)
  }

  it should "return an error when referencing a variable before it is defined" taggedAs(tnEngineTag, parseConfigTag) in {
    shouldContainNErrors(engine.parseCommands(fileReader.readConfiguration("src/test/resources/com/bfm/topnotch/tnengine/tooEarlyVarPlan.json"), fileReader), 1)
  }

  it should "return two errors when two of three commands are invalid" taggedAs(tnEngineTag, parseConfigTag) in {
    shouldContainNErrors(engine.parseCommands(fileReader.readConfiguration("src/test/resources/com/bfm/topnotch/tnengine/twoThirdsInvalidPlan.json"), fileReader), 2, 1)
  }

  it should "return an error when referencing a properly defined variable but stating that is a file on disk" taggedAs(tnEngineTag, parseConfigTag) in {
    shouldContainNErrors(engine.parseCommands(fileReader.readConfiguration("src/test/resources/com/bfm/topnotch/tnengine/varAsFilePlan.json"), fileReader), 1, 1)
  }

  it should "parse a correct, one command plan correctly" taggedAs(tnEngineTag, parseConfigTag) in {
    engine.parseCommands(fileReader.readConfiguration("src/test/resources/com/bfm/topnotch/tnengine/oneCorrectPlan.json"), fileReader) shouldBe oneCorrectPlan
  }

  it should "correctly parse a plan that includes diff, assertion, and view commands and that references and " +
    "stores cached and uncached variables" taggedAs(tnEngineTag, parseConfigTag) in {
    engine.parseCommands(fileReader.readConfiguration("src/test/resources/com/bfm/topnotch/tnengine/allCmdsPlan.json"), fileReader) shouldBe allCmdsPlan
  }

  "getInputDF" should "handle csv input data" taggedAs(tnEngineTag, getInputDFTag) in {
    val df = engine.getInputDF(Input("src/test/resources/com/bfm/topnotch/tnengine/rawTest.csv", true, Some(",")))
    df.count() shouldBe 4
  }

  "executeCommands" should "do nothing when given an empty seq of commands" taggedAs(tnEngineTag, executeCommandsTags) in {
    engine.executeCommands(Seq(), assertionRunner, diffCreator, viewCreator)
  }

  it should "cache two command outputs when the first is used by a view to generate the second" taggedAs(tnEngineTag, executeCommandsTags, cacheTag) in {
    engine.executeCommands(
      engine.parseCommands(fileReader.readConfiguration("src/test/resources/com/bfm/topnotch/tnengine/multicachePlan.json"), fileReader),
      assertionRunner, diffCreator, viewCreator)
    spark.sparkContext.getPersistentRDDs.size shouldBe 2
  }

  it should "run and write the correct parquet files when given a seq of commands that uses keys, " +
    "load from keys and files, and write to disk" taggedAs(tnEngineTag, executeCommandsTags) in {
    //we aren't testing what the assertionRunner puts in hbase, so accept anything
    //need to set hbase as assertionRunner has been configured to write to hbase
    setHBaseMock(new HTableParams(TnHBaseWriter.TABLE_NAME, Seq(null)), true)
    //the files we expect to be created in testOutput
    val dirsToBeCreated = Seq("testOutput", "diffOutput.parquet", "assertionOutput.parquet")
    //remove everything already there to make sure we are testing for the creation of new files
    FileUtils.cleanDirectory(outputDir)
    engine.executeCommands(allCmdsPlan, assertionRunner, diffCreator, viewCreator)
    val jFiles = FileUtils.listFilesAndDirs(outputDir, FalseFileFilter.INSTANCE, TrueFileFilter.INSTANCE)
    val files = jFiles.toArray(new Array[File](jFiles.size()))
    files.map(_.getName) should contain theSameElementsAs dirsToBeCreated
  }

  "run" should "execute correctly when given completePlan.json and no variables" taggedAs (tnEngineTag, runTag) in {
    //remove everything already there to make sure we are testing for the creation of new files
    FileUtils.cleanDirectory(outputDir)
    engine.run(TnCLIConfig("src/test/resources/com/bfm/topnotch/tnengine/completePlan.json"))
    val jFiles = FileUtils.listFilesAndDirs(outputDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE)
    val files = jFiles.toArray(new Array[File](jFiles.size()))
    files.map(_.getName) should contain allOf ("testOutput", "assertionKey", "diffOutput.parquet", "assertionOutput.parquet")
  }

  it should "execute correctly when given cliReplacementPlan.json and a variable" taggedAs (tnEngineTag, runTag) in {
    //remove everything already there to make sure we are testing for the creation of new files
    FileUtils.cleanDirectory(outputDir)
    engine.run(TnCLIConfig("src/test/resources/com/bfm/topnotch/tnengine/cliReplacementPlan.json",
      variableDictionary = Map("fileNameToReplace" -> "src/test/resources/com/bfm/topnotch/tnview/currentLoans.parquet")))
    val jFiles = FileUtils.listFilesAndDirs(outputDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE)
    val files = jFiles.toArray(new Array[File](jFiles.size()))
    val x = files.map(_.getName)
    files.map(_.getName) should contain allOf ("testOutput", "assertionKey", "assertionOutput.parquet")
  }

  it should "return 0 failures if all assertions pass" taggedAs(executeCommandsTags) in {
    engine.run(TnCLIConfig("src/test/resources/com/bfm/topnotch/tnengine/completePlan.json")) shouldBe 0
  }

  it should "return 1 when 1 assertion fails" taggedAs(executeCommandsTags) in {
    engine.run(TnCLIConfig("src/test/resources/com/bfm/topnotch/tnengine/completePlanBrokenThreshold.json")) shouldBe 1
  }
}
