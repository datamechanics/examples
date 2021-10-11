/*
 * Inspired by https://github.com/holdenk/sparkProjectTemplate.g8
 */
package co.datamechanics.pi

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Use this to test the app locally, from sbt:
  * sbt "run 100"
  *  (+ select LocalMain when prompted)
  */
object LocalMain extends App {
  val conf = new SparkConf()
    .setMaster("local")
  Runner.run(conf, args)
}

/**
  * Use this class when submitting the app to a cluster
  * */
object Main extends App {
  // the configuration of the app should supply all necessary config elements
  Runner.run(new SparkConf(), args)
}

object Runner {
  def run(conf: SparkConf, args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate()
    val slices = if (args.length > 0) args(0).toInt else 2
    val estimate = SparkPi.approximatePi(spark, slices)
    println(s"Pi is roughly ${estimate}")
    spark.stop()
  }
}
