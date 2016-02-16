import sbtsparksubmit.SparkSubmitPlugin.autoImport._

object SbtSparkSubmit {
  lazy val sparkTopNotchSettings =
    SparkSubmitSetting(
      SparkSubmitSetting("ExampleTopNotch",
        Seq(
          "--num-executors", "10",
          "--class", "com.bfm.topnotch.tnengine.TnEngine",
          "--files", "put files here"
        ),
        Seq(
          "planName.json"
        )
      )
    )
}