package com.yaoting117.learning.bigdata.sparkcore.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DistinctDemo {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("DistinctDemo").setMaster("local[*]")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val input: RDD[Int] = sparkContext.parallelize(Array(10, 20, 10, 40, 30, 90, 40))

//    val result: RDD[Int] = input.distinct()

    // (10,null)|(20,null)|(10,null)|(40,null)|(30,null)|(90,null)|(40,null)
    val rdd1: RDD[(Int, Null)] = input.map(x => (x, null))
    // (40,null)|(90,null)|(10,null)|(20,null)|(30,null)
    val rdd2: RDD[(Int, Null)] = rdd1.reduceByKey((x, _) => x)
    // 40|90|10|20|30
    val result: RDD[Int] = rdd2.map(_._1)

//    val result: RDD[Int] = input.map(x => (x, null)).reduceByKey((x, _) => x).map(_._1)

    println(result.collect().mkString("|"))


    sparkContext.stop()

  }

}
