package com.yaoting117.learning.bigdata.sparkcore.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 *  集合操作
 *    union
 *    intersection
 *    subtract
 */
object CollectionDemo {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("FlatMapDemo").setMaster("local[*]")
    val sparkContext: SparkContext = new SparkContext(sparkConf)




    sparkContext.stop()

  }

}
