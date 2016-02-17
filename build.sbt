name := "topnotch"

version := "0.1"

scalaVersion := "2.10.6"

libraryDependencies ++= Dependencies.Bundles.allDependencies

resolvers ++= Seq(
  Resolver.bintrayRepo("iheartradio", "maven"),
  "Twitter Maven" at "http://maven.twttr.com"
)

SbtSparkSubmit.sparkTopNotchSettings

test in assembly := {}

parallelExecution in Test := false

enablePlugins(SparkSubmitYARN)