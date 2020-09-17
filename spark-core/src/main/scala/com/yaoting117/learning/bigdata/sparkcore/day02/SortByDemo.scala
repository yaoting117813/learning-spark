package com.yaoting117.learning.bigdata.sparkcore.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortByDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("FlatMapDemo").setMaster("local[*]")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val input: RDD[Int] = sparkContext.parallelize(List(4, 10, 22, 14, 82))

//    val result: RDD[Int] = input.sortBy(x => x, ascending = true)
    val result: RDD[Int] = input.sortBy(x => x + "", ascending = true)

    println(result.collect().mkString(" "))

    sparkContext.stop()
  }

}
