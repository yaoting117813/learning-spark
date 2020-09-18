package com.yaoting117.learning.bigdata.sparkcore.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *  数据不是Driver发送到Executor 而是Executor到Driver拉取
 *  groupByKey().mapValues(_.sum)
 *  reduceByKey(_ + _)
 *    merging locally on each mapper before sending results to a reducer 减少网络传输
 */
object ReduceByKeyDemo {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("FlatMapDemo").setMaster("local[*]")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val nameWithScoreList: List[(String, Int)] = List(("yaoting117", 100), ("yaoting117", 80), ("yaoting117", 45), ("qianqian813", 99), ("qianqian813", 98))

    val input: RDD[(String, Int)] = sparkContext.parallelize(nameWithScoreList)

    println(input.groupByKey().mapValues(value => value.sum).collect().mkString("|"))
    println("-----------------------------")
    println(input.reduceByKey(_ + _).collect().mkString("|"))

    sparkContext.stop()

  }

}
