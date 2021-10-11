val sparkVersionValue = "3.0.1"

ThisBuild / scalaVersion     := "2.12.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "co.datamechanics"
ThisBuild / organizationName := "datamechanics"

lazy val root = (project in file("."))
  .settings(
    name := "datamechanics-demo",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersionValue % "provided"
    ),
  )

// uses compile classpath for the run task, including "provided" jar (cf http://stackoverflow.com/a/21803413/3827)
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated
runMain in Compile := Defaults.runMainTask(fullClasspath in Compile, runner in(Compile, run)).evaluated

assemblyJarName in assembly := "application.jar"
