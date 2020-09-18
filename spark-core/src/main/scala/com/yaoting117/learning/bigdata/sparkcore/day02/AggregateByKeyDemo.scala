package com.yaoting117.learning.bigdata.sparkcore.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKeyDemo {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("FlatMapDemo").setMaster("local[*]")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val input: RDD[(String, Int)] = sparkContext.parallelize(List(("cat", 2), ("cat", 5), ("mouse", 4), ("cat", 10), ("dog", 8), ("mouse", 6)), 2)

    // def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U, combOp: (U, U) => U): RDD[(K, U)]

    //
//    val result: RDD[(String, Int)] = input.aggregateByKey(0)(_ + _, _ + _)


    // (dog,8)|(cat,15)|(mouse,10)
//    val result: RDD[(String, Int)] = input.aggregateByKey(0)(math.max, _ + _)

    // (dog,100)|(cat,200)|(mouse,200)
    val result: RDD[(String, Int)] = input.aggregateByKey(100)(math.max, _ + _)

    println(result.collect().mkString("|"))


    sparkContext.stop()

  }

}
