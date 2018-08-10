import sbt.Keys.{name, scalaVersion, startYear, version}
import sbt.inThisBuild


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

}