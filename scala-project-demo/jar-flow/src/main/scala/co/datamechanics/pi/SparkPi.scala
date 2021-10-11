package co.datamechanics.pi

import scala.math.random

import org.apache.spark.sql.SparkSession

object SparkPi {
  def approximatePi(spark: SparkSession, slices: Int): Double = {
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        if (x*x + y*y <= 1) 1 else 0
    }.reduce(_ + _)
    4.0 * count / (n - 1)
  }
}
