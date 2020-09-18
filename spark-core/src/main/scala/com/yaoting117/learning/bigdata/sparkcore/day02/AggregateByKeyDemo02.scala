package com.yaoting117.learning.bigdata.sparkcore.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * aggregateByKey 详解
 * Initial value or Zero value
 * It can be 0 if aggregation is type of sum of all values
 * We have have this value as Double.MaxValue if aggregation objective is to find minimum value
 * We can also use Double.MinValue value if aggregation objective is to find maximum value
 * Or we can also have an empty List or Map object, if we just want a respective collection as an output for each key
 * Sequence operation function which transforms/merges data of one type [V] to another type [U]
 * Combination operation function which merges multiple transformed type [U] to a single type [U]
 */
object AggregateByKeyDemo02 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("aggregateByKey").setMaster("local[*]")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val studentRDD: RDD[(String, String, Int)] = sparkContext.parallelize(
      Array(
        ("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91), ("Joseph", "Biology", 82),
        ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62), ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80),
        ("Tina", "Maths", 78), ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87),
        ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91), ("Thomas", "Biology", 74),
        ("Cory", "Maths", 56), ("Cory", "Physics", 65), ("Cory", "Chemistry", 71), ("Cory", "Biology", 68),
        ("Jackeline", "Maths", 86), ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75), ("Jackeline", "Biology", 83),
        ("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64), ("Juan", "Biology", 60)
      ), 3
    )

    /**
     * (Tina,87)
     * (Thomas,93)
     * (Jackeline,86)
     * (Joseph,91)
     * (Juan,69)
     * (Jimmy,97)
     * (Cory,71)
     */
    //    val result: RDD[(String, Int)] = maxScorePreStudent(studentRDD)

    /**
     * (Tina,(Biology,87))
     * (Thomas,(Physics,93))
     * (Jackeline,(Maths,86))
     * (Joseph,(Chemistry,91))
     * (Juan,(Physics,69))
     * (Jimmy,(Chemistry,97))
     * (Cory,(Chemistry,71))
     */
    //    val result: RDD[(String, (String, Int))] = maxScorePreStudentWithSubject(studentRDD)

    val result: RDD[(String, Double)] = percentageScore(studentRDD)
    result.collect().foreach(println)

    sparkContext.stop()

  }

  /**
   * 每个学生最高分数
   *
   * @param studentRDD [(学生姓名, 科目名称, 分数)]
   * @return RDD[(学生姓名, 最高分数)]
   */
  def maxScorePreStudent(studentRDD: RDD[(String, String, Int)]): RDD[(String, Int)] = {
    val zeroValue: Int = 0

    val seqOp: (Int, (String, Int)) => Int = (accumulator: Int, element: (String, Int)) =>
      if (accumulator > element._2) accumulator else element._2

    val combOp: (Int, Int) => Int = (accumulator1: Int, accumulator2: Int) =>
      if (accumulator1 > accumulator2) accumulator1 else accumulator2

    val result: RDD[(String, Int)] = studentRDD.map(t => (t._1, (t._2, t._3))).aggregateByKey(zeroValue)(seqOp, combOp)

    result
  }

  /**
   * 学生最高分数,是哪一门科目
   *
   * @param studentRDD
   * @return RDD[(学生姓名, (科目,最高分数))]
   */
  def maxScorePreStudentWithSubject(studentRDD: RDD[(String, String, Int)]): RDD[(String, (String, Int))] = {
    val zeroValue: (String, Int) = ("", 0)

    val seqOp: ((String, Int), (String, Int)) => (String, Int) = (accumulator: (String, Int), element: (String, Int)) =>
      if (accumulator._2 > element._2) accumulator else element

    val combOp: ((String, Int), (String, Int)) => (String, Int) = (accumulator1: (String, Int), accumulator2: (String, Int)) =>
      if (accumulator1._2 > accumulator2._2) accumulator1 else accumulator2

    val result: RDD[(String, (String, Int))] = studentRDD.map(t => (t._1, (t._2, t._3))).aggregateByKey(zeroValue)(seqOp, combOp)

    result
  }

  /**
   * 每个学生的平均分数
   *
   * @param studentRDD
   * @return
   */
  def percentageScore(studentRDD: RDD[(String, String, Int)]): RDD[(String, Double)] = {
    val zeroValue: (Int, Int) = (0, 0)

    def seqOp: ((Int, Int), (String, Int)) => (Int, Int) = (accumulator: (Int, Int), element: (String, Int)) =>
      (accumulator._1 + element._2, accumulator._2 + 1)

    def combOp: ((Int, Int), (Int, Int)) => (Int, Int) = (accumulator1: (Int, Int), accumulator2: (Int, Int)) =>
      (accumulator1._1 + accumulator2._1, accumulator1._2 + accumulator2._2)

    val result: RDD[(String, Double)] =
      studentRDD
        .map(t => (t._1, (t._2, t._3))) // (学生姓名, (科目名称, 科目分数))
        .aggregateByKey(zeroValue)(seqOp, combOp) // (学生姓名, (总分数, 科目数目))
        .map(t => (t._1, t._2._1 / t._2._2 * 1.0)) // (学生姓名, 平均分数)
    result
  }

}
