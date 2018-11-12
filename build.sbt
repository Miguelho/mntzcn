import ProjectBuild.project._

scalaVersion := "2.11.12"

scalacOptions ++= Seq("-deprecation")

resolvers ++= {
  Seq(
    Resolver.sonatypeRepo("public"),
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    "CDH" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
  )
}

libraryDependencies += "junit" % "junit" % "4.10" % Test
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "org.apache.spark" %% "spark-mllib" % "2.3.0",
  "joda-time" % "joda-time" % "2.10"

)

lazy val mntzn = (project in file(".")).settings(buildSettings)