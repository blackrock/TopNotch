package com.bfm.topnotch.tnengine

import com.bfm.topnotch.tnassertion.{TnAssertionSeq, TnAssertionCmd, TnAssertionParams, TnAssertionRunner}
import com.bfm.topnotch.tndiff.{TnDiffInput, TnDiffParams, TnDiffCmd, TnDiffCreator}
import com.bfm.topnotch.tnview.{TnViewParams, TnViewCmd, TnViewCreator}
import com.typesafe.config.{ConfigValueFactory, ConfigFactory, Config}
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{FalseFileFilter, TrueFileFilter}
import org.scalatest.{Matchers, Tag}
import com.bfm.topnotch.SparkApplicationTester
import java.io.File

/**
 * The tests for [[com.bfm.topnotch.tnengine.TnEngine TnEngine]].
 */
class TnEngineTest extends SparkApplicationTester with Matchers {

  lazy val engine = new TnEngine(sqlContext)
  lazy val diffCreator = new TnDiffCreator()
  lazy val assertionRunner = new TnAssertionRunner(new TnHBasePersister(Some(hconn)))
  lazy val viewCreator = new TnViewCreator(sqlContext)
  val outputDir = new File(getClass.getResource("testOutput").getFile)

  /**
   * The tags
   */
  object tnEngineTag extends Tag("TnEngine")
  object collectErrorsTag extends Tag("collectErrors")
  object executeCommandsTags extends Tag("executeCommands")
  object parseConfigTag extends Tag("parseConfig")
  object getInputDFTag extends Tag("getInputDF")

  /**
   * The plans
   */
  //this plan is complete, just 3 commands, but tests all features and should be the same as test/resources/com/bfm/topnotch/tnengine/completePlan.json
  val completePlan = Seq[TnCmd](
    TnViewCmd(
      TnViewParams(Seq("loanData"), "select * from loanData"),
      Seq(TnInput("src/test/resources/com/bfm/topnotch/tnview/currentLoans.parquet", true)),
      "viewKey",
      true
    ),
    TnDiffCmd(
      TnDiffParams(
        TnDiffInput(Seq("loanID", "poolNum"), Seq("loanBal")),
        TnDiffInput(Seq("loanIDOld", "poolNumOld"), Seq("loanBalOld"))
      ),
      TnInput("src/test/resources/com/bfm/topnotch/tndiff/currentLoans.parquet", true),
      "cur",
      TnInput("src/test/resources/com/bfm/topnotch/tndiff/oldLoans.parquet", true),
      "old",
      false,
      "diffKey",
      false,
      numericThreshold = 1e-6,
      Some("target/scala-2.10/test-classes/com/bfm/topnotch/tnengine/testOutput/diffOutput.parquet")
    ),
    TnAssertionCmd(
      TnAssertionSeq(
        Seq(
          TnAssertionParams("loanBal > 0", "Loan balances are positive", 0.01),
          TnAssertionParams("loanBal > 1", "Loan balances are greater than 1", 0.02)
        )
      ),
      TnInput("viewKey", false),
      "assertionKey",
      false,
      Some("target/scala-2.10/test-classes/com/bfm/topnotch/tnengine/testOutput/assertionOutput.parquet")
    )
  )

  //this plan has 2 errors and 1 correct command
  val (firstError, secondError) = (TnErrorCmd("first error", 0), TnErrorCmd("second error", 2))
  val errorPlan = Seq[TnCmd](firstError, completePlan(0), secondError, completePlan(0))
  //this plan has one command that is correct. It should be the same as test/resources/com/bfm/topnotch/tnengine/oneCorrectPlan.json
  val oneCorrectPlan = completePlan.slice(0, 1)

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
    engine.collectErrors(completePlan) shouldBe None
  }

  it should "return only the errors as one string when given a list of commands with multiple errors " +
    "and correct commnds" taggedAs(tnEngineTag, collectErrorsTag) in {
    engine.collectErrors(errorPlan) shouldBe Some(firstError.toString + secondError.toString)
  }

  /**
   * Parse the config file and add the necessary path variable.
   */
  def parseConfigAddPath(configFile: String): Config = {
    val file = new File(configFile)
    ConfigFactory.parseFile(file).withValue("path", ConfigValueFactory.fromAnyRef(file.getParentFile.getAbsolutePath))
  }

  "parseCommands" should "throw an exception when given a config missing the topnotch namespace" taggedAs(tnEngineTag, parseConfigTag) in {
    intercept[IllegalArgumentException] {
      engine.parseCommands(ConfigFactory.empty()) shouldBe Seq()
    }
  }

  it should "return None when the topnotch namespace is empty" taggedAs(tnEngineTag, parseConfigTag) in {
    engine.parseCommands(parseConfigAddPath("src/test/resources/com/bfm/topnotch/tnengine/emptyPlan.json")) shouldBe Seq()
  }

  it should "return an error when referencing a nonexistant command" taggedAs(tnEngineTag, parseConfigTag) in {
    shouldContainNErrors(engine.parseCommands(parseConfigAddPath("src/test/resources/com/bfm/topnotch/tnengine/noCmdPlan.json")), 1)
  }

  it should "return an error when referencing a nonexistant file" taggedAs(tnEngineTag, parseConfigTag) in {
    shouldContainNErrors(engine.parseCommands(parseConfigAddPath("src/test/resources/com/bfm/topnotch/tnengine/noFilePlan.json")), 1)
  }

  it should "return an error when referencing a nonexistant variable" taggedAs(tnEngineTag, parseConfigTag) in {
    shouldContainNErrors(engine.parseCommands(parseConfigAddPath("src/test/resources/com/bfm/topnotch/tnengine/noVarPlan.json")), 1)
  }

  it should "return an error when referencing a variable before it is defined" taggedAs(tnEngineTag, parseConfigTag) in {
    shouldContainNErrors(engine.parseCommands(parseConfigAddPath("src/test/resources/com/bfm/topnotch/tnengine/tooEarlyVarPlan.json")), 1)
  }

  it should "return two errors when two of three commands are invalid" taggedAs(tnEngineTag, parseConfigTag) in {
    shouldContainNErrors(engine.parseCommands(parseConfigAddPath("src/test/resources/com/bfm/topnotch/tnengine/twoThirdsInvalidPlan.json")), 2, 1)
  }

  it should "parse a correct, one command plan correctly" taggedAs(tnEngineTag, parseConfigTag) in {
    engine.parseCommands(parseConfigAddPath("src/test/resources/com/bfm/topnotch/tnengine/oneCorrectPlan.json")) shouldBe oneCorrectPlan
  }

  it should "correctly parse a plan that includes diff, assertion, and view commands and that references and " +
    "stores cached and uncached variables" taggedAs(tnEngineTag, parseConfigTag) in {
    engine.parseCommands(parseConfigAddPath("src/test/resources/com/bfm/topnotch/tnengine/completePlan.json")) shouldBe completePlan
  }

  "getInputDF" should "handle csv input data" taggedAs(tnEngineTag, getInputDFTag) in {
    val df = engine.getInputDF(TnInput("src/test/resources/com/bfm/topnotch/tnengine/rawTest.csv", true, ","))
    df.count() shouldBe 4
  }

  "executeCommands" should "do nothing when given an empty seq of commands" taggedAs(tnEngineTag, executeCommandsTags) in {
    engine.executeCommands(Seq(), assertionRunner, diffCreator, viewCreator)
  }

  it should "run and write the correct parquet files when given a seq of commands that store variables, load them, " +
    "and write to disk" taggedAs(tnEngineTag, executeCommandsTags) in {
    //we aren't testing what the assertionRunner puts in hbase, so accept anything
    setHBaseMock(new HTableParams(TnHBasePersister.TABLE_NAME, Seq(null)), true)
    //the files we expect to be created in testOutput
    val dirsToBeCreated = Seq("testOutput", "diffOutput.parquet", "assertionOutput.parquet")
    FileUtils.cleanDirectory(outputDir)
    //remove everything already there to make sure we are testing for the creation of new files
    engine.executeCommands(completePlan, assertionRunner, diffCreator, viewCreator)
    val jFiles = FileUtils.listFilesAndDirs(outputDir, FalseFileFilter.INSTANCE, TrueFileFilter.INSTANCE)
    val files = jFiles.toArray(new Array[File](jFiles.size()))
    files.map(_.getName) should contain theSameElementsAs dirsToBeCreated
  }
}
