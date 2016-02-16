name := "topnotch"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Dependencies.Bundles.allDependencies

resolvers ++= Seq(
  Resolver.bintrayRepo("iheartradio", "maven"),
  "Twitter Maven" at "http://maven.twttr.com"
)

SbtSparkSubmit.sparkTopNotchSettings


parallelExecution in Test := false

enablePlugins(SparkSubmitYARN)