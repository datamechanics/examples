/*
 * Inspired by https://github.com/holdenk/sparkProjectTemplate.g8
 */
package co.datamechanics.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Use this to test the app locally, from sbt:
  * sbt "run inputFile.txt outputFile.txt"
  *  (+ select LocalMain when prompted)
  */
object LocalMain extends App {
  val (inputFile, outputFile) = (args(0), args(1))
  val conf = new SparkConf()
    .setMaster("local")

  Runner.run(conf, inputFile, outputFile)
}

/**
  * Use this class when submitting the app to a cluster
  * */
object Main extends App {
  val (inputFile, outputFile) = (args(0), args(1))

  // the configuration of the app should supply all necessary config elements
  Runner.run(new SparkConf(), inputFile, outputFile)
}

object Runner {
  def run(conf: SparkConf, inputFile: String, outputFile: String): Unit = {
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate()
    val rdd = spark.sparkContext.textFile(inputFile)
    val counts = WordCount.withStopWordsFiltered(rdd)
    counts.saveAsTextFile(outputFile)
    spark.stop()
  }
}
