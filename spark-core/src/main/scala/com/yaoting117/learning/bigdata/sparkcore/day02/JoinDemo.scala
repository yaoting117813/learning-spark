package com.yaoting117.learning.bigdata.sparkcore.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  Join操作
 *    join
 *    leftOuterJoin
 *    rightOuterJoin
 *    fullOuterJoin
 */
object JoinDemo {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("FlatMapDemo").setMaster("local[*]")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val rdd1: RDD[(String, Int)] = sparkContext.parallelize(List(("yaoting", 50), ("qianqian", 60), ("yaoting", 80), ("qianqian", 90), ("zhangshang", 100)))
    val rdd2: RDD[(String, Int)] = sparkContext.parallelize(List(("yaoting", 70), ("qianqian", 100), ("lixi", 20)))

    // (qianqian,(60,100))|(qianqian,(90,100))|(yaoting,(50,70))|(yaoting,(80,70))
    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    // (zhangshang,(100,None))|(qianqian,(60,Some(100)))|(qianqian,(90,Some(100)))|(yaoting,(50,Some(70)))|(yaoting,(80,Some(70)))
    val leftOuterJoinRDD: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)

    // (qianqian,(Some(60),100))|(qianqian,(Some(90),100))|(yaoting,(Some(50),70))|(yaoting,(Some(80),70))|(lixi,(None,20))
    val rightOuterJoinRDD: RDD[(String, (Option[Int], Int))] = rdd1.rightOuterJoin(rdd2)

    // (zhangshang,(Some(100),None))|(qianqian,(Some(60),Some(100)))|(qianqian,(Some(90),Some(100)))|(yaoting,(Some(50),Some(70)))|(yaoting,(Some(80),Some(70)))|(lixi,(None,Some(20)))
    val fullOuterJoinRDD: RDD[(String, (Option[Int], Option[Int]))] = rdd1.fullOuterJoin(rdd2)

    println(fullOuterJoinRDD.collect().mkString("|"))


    sparkContext.stop()

  }

}
