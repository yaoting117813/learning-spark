package com.yaoting117.learning.bigdata.sparkcore.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark combineByKey is a generic function to combine
 * the elements for each key using a custom set of aggregation functions
 * createCombiner
 * This function is a first argument of combineByKey function
 * It is a first aggregation step for each key
 * It will be executed when any new key is found in a partition
 * Execution of this lambda function is local to a partition of a node, on each individual values
 * mergeValue
 * mergeCombiner functions
 */
object CombineByKeyDemo02 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("FlatMapDemo").setMaster("local[*]")
    val sparkContext: SparkContext = new SparkContext(sparkConf)


    val studentRDD: RDD[(String, String, Int)] = sparkContext.parallelize(Array(
      ("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91),
      ("Joseph", "Biology", 82), ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62),
      ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80), ("Tina", "Maths", 78),
      ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87),
      ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91),
      ("Thomas", "Biology", 74), ("Cory", "Maths", 56), ("Cory", "Physics", 65),
      ("Cory", "Chemistry", 71), ("Cory", "Biology", 68), ("Jackeline", "Maths", 86),
      ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75), ("Jackeline", "Biology", 83),
      ("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64),
      ("Juan", "Biology", 60)), 3)

    // (分数, 1)

    /*def createCombiner: ((String, Int)) => (Double, Int) = (tuple: (String, Int)) =>
      (tuple._2.toDouble, 1)*/
    val createCombiner: ((String, Int)) => (Double, Int) = (tuple: (String, Int)) =>
      (tuple._2.toDouble, 1)

    // (分区内的总分数, 分区内的总科目数)
    /*def mergeValue: ((Double, Int), (String, Int)) => (Double, Int) =
      (accumulator: (Double, Int), element: (String, Int)) =>
        (accumulator._1 + element._2, accumulator._2 + 1)*/
    val mergeValue: ((Double, Int), (String, Int)) => (Double, Int) =
      (accumulator: (Double, Int), element: (String, Int)) =>
        (accumulator._1 + element._2, accumulator._2 + 1)

    /*def mergeCombiner: ((Double, Int), (Double, Int)) => (Double, Int) =
      (accumulator1: (Double, Int), accumulator2: (Double, Int)) =>
        (accumulator1._1 + accumulator2._1, accumulator1._2 + accumulator2._2)*/
    //(总分数, 总科目数)
    val mergeCombiner: ((Double, Int), (Double, Int)) => (Double, Int) =
      (accumulator1: (Double, Int), accumulator2: (Double, Int)) =>
        (accumulator1._1 + accumulator2._1, accumulator1._2 + accumulator2._2)

    val result: RDD[(String, Double)] = studentRDD
      .map(t => (t._1, (t._2, t._3)))
      .combineByKey(createCombiner, mergeValue, mergeCombiner)
      .map(e => (e._1, e._2._1 / e._2._2))

    result.collect.foreach(println)

    sparkContext.stop()
  }

}
