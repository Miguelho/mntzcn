import ProjectBuild.project._
import sbt.Keys.libraryDependencies

scalaVersion := "2.11.12"

scalacOptions ++= Seq("-deprecation")

resolvers ++= {
  Seq(
    Resolver.sonatypeRepo("public"),
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    "CDH" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
  )
}

lazy val dependenciesSettings = {
  val sparkVersion = "2.3.0"
  val sparkModules = Seq("core", "mllib", "hive")

  def sparkString2SbtModule(s: String, sparkVersion: String): sbt.ModuleID = {
    val moduleName = "spark-"+ s
    "org.apache.spark" %% moduleName % sparkVersion % "provided"
  }

  Seq(
    libraryDependencies ++= sparkModules.map(m => sparkString2SbtModule(m, sparkVersion)),
    libraryDependencies += "joda-time" % "joda-time" % "2.10",
    libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
    libraryDependencies += "junit" % "junit" % "4.10" % Test,
    libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5" % Test,
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test
  )
}

lazy val mntzn = (project in file(".")).settings(buildSettings).settings(assemblySettings).settings(dependenciesSettings)