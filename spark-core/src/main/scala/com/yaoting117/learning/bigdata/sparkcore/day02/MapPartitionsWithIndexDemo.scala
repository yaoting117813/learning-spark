package com.yaoting117.learning.bigdata.sparkcore.day02

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object MapPartitionsWithIndexDemo {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("MapPartitionsWithIndexDemo").setMaster("local[*]")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    // sparkContext.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val input: RDD[Int] = sparkContext.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3)

    val func: (Int, Iterator[Int]) => Iterator[String] = (index: Int, it: Iterator[Int]) => {
      it.map(e => s"partitionId: $index, value $e")
    }

    // def mapPartitions[U: ClassTag](f: Iterator[T] => Iterator[U], preservesPartitioning: Boolean = false): RDD[U]
    // def mapPartitionsWithIndex[U: ClassTag](f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean = false)
    input.mapPartitionsWithIndex((index, it) => {

//      it.foreach(e => println(index + ", " +  e))

      val strings: Iterator[String] = func(index, it)
      strings.foreach(println(_))

      it.map(_ * 10)
    }).count()

    sparkContext.stop()

  }


}
