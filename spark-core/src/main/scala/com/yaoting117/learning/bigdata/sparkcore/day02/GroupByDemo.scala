package com.yaoting117.learning.bigdata.sparkcore.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  分区 分组
 *    一个分区可能有多个分组,key相同的一定在一个分组内,也在一个分组类
 *    partition1  ("yaoting117", CompactBuffer(.....))
 *                ("qianqian813", CompactBuffer(.....))
 *    partition2  ("zhoumiao888", CompactBuffer(.....))
 *                ("hahaha", CompactBuffer(.....))\
 *    ......
 */
object GroupByDemo {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("FlatMapDemo").setMaster("local[*]")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val input: RDD[(String, Any)] = sparkContext.parallelize(List(
      ("yaoting117", 37), ("yaoting117", "沙市"), ("qianqian813", 26), ("qianqian813", "武汉"), ("qianqian813", "北京"))
    )

    // ShuffledRDD
    val result: RDD[(String, Iterable[(String, Any)])] = input.groupBy(_._1)


    val result2: RDD[(String, Iterable[Any])] = input.groupByKey()

    println(result.collect().mkString("|"))

    sparkContext.stop()

  }

}
