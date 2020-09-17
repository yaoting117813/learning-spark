package com.yaoting117.learning.bigdata.sparkcore.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FlatMapDemo {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("FlatMapDemo").setMaster("local[*]")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val input: RDD[List[String]] = sparkContext.parallelize(List(List("java scala python", "javascript c++ shell"), List("vue angular react", "spring-cloud spring-boot")))

    // RDD -> def flatMap[U : ClassTag](f: T => scala.TraversableOnce[U]): RDD[U]
    // List -> final override def flatMap[B, That](f: A => GenTraversableOnce[B])(implicit bf: CanBuildFrom[List[A], B, That]): That
    val result: RDD[String] = input.flatMap(_.flatMap(_.split(" ")))

    println(result.collect().mkString(" "))



    sparkContext.stop()

  }

}
