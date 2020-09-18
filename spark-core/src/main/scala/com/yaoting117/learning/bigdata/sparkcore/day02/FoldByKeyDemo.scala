package com.yaoting117.learning.bigdata.sparkcore.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FoldByKeyDemo {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("FlatMapDemo").setMaster("local[*]")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val input: RDD[(String, Int)] = sparkContext.parallelize(List(("cat", 2), ("cat", 5), ("mouse", 4), ("cat", 10), ("dog", 8), ("mouse", 6)), 2)

    val result: RDD[(String, Int)] = input.foldByKey(0)(_ + _)

    println(result.collect().mkString("|"))


    sparkContext.stop()

  }

}
