package com.yaoting117.learning.bigdata.sparkcore.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {


    // 创建SparkContext
    val conf: SparkConf = new SparkConf().setAppName("WordCount")
    val sparkContext: SparkContext = new SparkContext(conf)

    // 创建RDD
    val input: RDD[String] = sparkContext.textFile(args(0))

    // 对RDD执行各种转换操作 (transformation)
    val words: RDD[String] = input.flatMap(_.split(" "))

    val wordWithOne: RDD[(String, Int)] = words.map((_, 1))

    val reducedWordCount: RDD[(String, Int)] = wordWithOne.reduceByKey(_ + _)

    val sortedWordCount: RDD[(String, Int)] = reducedWordCount.sortBy(_._2, ascending = false)

    // 对RDD执行动作操作(action)
    sortedWordCount.saveAsTextFile(args(1))

    // 释放资源
    sparkContext.stop()

  }

}
