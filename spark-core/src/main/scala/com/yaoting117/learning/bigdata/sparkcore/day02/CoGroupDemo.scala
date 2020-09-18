package com.yaoting117.learning.bigdata.sparkcore.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  reduceByKey 效率最高
 *  coGroup
 */
object CoGroupDemo {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("FlatMapDemo").setMaster("local[*]")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val rdd1: RDD[(String, Int)] = sparkContext.parallelize(List(("yaoting", 50), ("qianqian", 60), ("yaoting", 80), ("qianqian", 90), ("zhangshang", 100)))
    val rdd2: RDD[(String, Int)] = sparkContext.parallelize(List(("yaoting", 70), ("qianqian", 100), ("lixi", 20)))

    // (zhangshang,(CompactBuffer(100),CompactBuffer()))|(qianqian,(CompactBuffer(60, 90),CompactBuffer(100)))|(yaoting,(CompactBuffer(50, 80),CompactBuffer(70)))|(lixi,(CompactBuffer(),CompactBuffer(20)))
    // fullOuterJoin 每一个rdd中的数据为 CompactBuffer(...)
    val result: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)

    // (zhangshang,100)|(qianqian,250)|(yaoting,200)|(lixi,20)
    /*val rdd3: RDD[(String, Int)] = rdd1.reduceByKey(_ + _)
    val rdd4: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)
    val rdd5: RDD[(String, Int)] = rdd3.union(rdd4)
    val resultRDD: RDD[(String, Int)] = rdd5.reduceByKey(_ + _)

    println(resultRDD.collect().mkString("|"))*/

    // (zhangshang,100)|(qianqian,250)|(yaoting,200)|(lixi,20)
    val rdd6: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
    val rdd7: RDD[(String, Int)] = rdd6.mapValues(t => t._1.sum + t._2.sum)

    println(rdd7.collect().mkString("|"))

//    println(result.collect().mkString("|"))


    sparkContext.stop()

  }

}
