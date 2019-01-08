import sbt.Keys.{name, scalaVersion, startYear, version}
import sbt.inThisBuild
import _root_.sbtassembly.AssemblyPlugin.autoImport._
import _root_.sbtassembly.PathList


class Project

object ProjectBuild {
  val project = new Project with ProjectSettings

  val printBuild: Unit = {
    println("\t*********")
    println(s"Project name: ${project.projectName}")
    println(s"Project version: ${project.projectVersion}")
    println(s"Project version: ${project.mainClass}")
    println("\t*********")
  }

}

trait ProjectSettings {
  val projectName = "Mntzcn"
  val projectVersion = "0.1-SNAPSHOT"
  val mainClass = "org.miguelho.kmeans.Main"

  lazy val buildSettings = inThisBuild(Seq(
    name := projectName,
    version := projectVersion,
    scalaVersion := "2.11.8",
    startYear := Some(2018)
  ))

  lazy val assemblySettings = Seq(
    assemblyMergeStrategy in assembly :={
      case PathList("META-INF", "services", xs@_*) => MergeStrategy.first // keep Lucene's services
      case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      case m if m.startsWith("META-INF") => MergeStrategy.discard
      case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
      case PathList("org", "apache", xs@_*) => MergeStrategy.first
      case PathList("org", "jboss", xs@_*) => MergeStrategy.first
      case PathList("org", "slf4j", xs@_*) => MergeStrategy.first
      case "about.html" => MergeStrategy.rename
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )

}