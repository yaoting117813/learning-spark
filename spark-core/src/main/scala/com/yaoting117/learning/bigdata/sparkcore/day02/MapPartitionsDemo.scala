package com.yaoting117.learning.bigdata.sparkcore.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionsDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("MapPartitionsDemo").setMaster("local[*]")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    // sparkContext.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val input: RDD[Int] = sparkContext.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3)

    // def map[U: ClassTag](f: T => U): RDD[U]
    val rdd1: RDD[Int] = input.map(i => i * 10)

    // def mapPartitions[U: ClassTag](f: Iterator[T] => Iterator[U], preservesPartitioning: Boolean = false): RDD[U]
    val rdd2: RDD[Int] = input.mapPartitions(it => it.map(i => i * 10))

    /*

      以上两种方式结果一样
        mapPartitions使用场景:
          写入数据库
            一个分区创建一个数据库连接
          请求外部资源
            一个分区创建一个网络连接



     */




    sparkContext.stop()
  }
}
