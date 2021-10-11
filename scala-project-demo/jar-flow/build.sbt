val sparkVersionValue = "2.4.5"

scalaVersion := "2.11.12"
organization := "co.datamechanics"
name := "datamechanics-demo"
version := "0.1.0-SNAPSHOT"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersionValue % "provided"

// uses compile classpath for the run task, including "provided" jar (cf http://stackoverflow.com/a/21803413/3827)
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated
runMain in Compile := Defaults.runMainTask(fullClasspath in Compile, runner in(Compile, run)).evaluated
