package com.yaoting117.learning.bigdata.sparkcore.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  使用CombineByKey实现WordCount
 */
object CombineByKeyDemo {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("FlatMapDemo").setMaster("local[*]")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    // 使用CombineByKey实现WordCount
    /*val input: RDD[(String, Int)] = sparkContext.parallelize(List(("cat", 2), ("cat", 5), ("mouse", 4), ("cat", 10), ("dog", 8), ("mouse", 6)), 2)

    // (dog,8)|(cat,17)|(mouse,10)
    val result: RDD[(String, Int)] = input.combineByKey(
      x => x,                     // 初始值函数  createCombiner
      (a: Int, b: Int) => a + b,  // 分区内聚合  mergeValue
      (m: Int, n: Int) => m + n   // 分区间聚合  mergeCombiners
    )

    println(result.collect().mkString("|"))*/

    // 深入理解 CombineByKey
    val rdd1: RDD[String] = sparkContext.parallelize(List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee"), 3)
    val rdd2: RDD[Int] = sparkContext.parallelize(List(1, 1, 2, 2, 2, 1, 2, 2, 2), 3)
    val rdd3: RDD[(Int, String)] = rdd2.zip(rdd1)

    // (1,dog)|(1,cat)|(2,gnu)|(2,salmon)|(2,rabbit)|(1,turkey)|(2,wolf)|(2,bear)|(2,bee)
//    println(rdd3.collect().mkString("|"))

    // (1,List(dog, cat, turkey))|(2,List(gnu, salmon, rabbit, wolf, bear, bee))
    val result: RDD[(Int, List[String])] = rdd3.combineByKey(
      x => List(x),
      (list: List[String], str: String) => list :+ str,
      (list1: List[String], list2: List[String]) => list1 ++ list2
    )

    println(result.collect().mkString("|"))

    sparkContext.stop()

  }

}
