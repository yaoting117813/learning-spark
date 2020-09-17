package com.yaoting117.learning.bigdata.sparkcore.day01.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class LambdaWordCount {

    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "yaoting117");

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("JavaLambdaWordCount");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = jsc.textFile("G:\\0916\\big-data\\learning-spark\\input\\word.txt");

        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairRDD<String, Integer> wordWithOne = words.mapToPair(word -> Tuple2.apply(word, 1));

        JavaPairRDD<String, Integer> reducedWordCount = wordWithOne.reduceByKey(Integer::sum);

        JavaPairRDD<Integer, String> swapedWordCount = reducedWordCount.mapToPair(Tuple2::swap);

        JavaPairRDD<Integer, String> sortedSwapedWordCount = swapedWordCount.sortByKey(false);

        JavaPairRDD<String, Integer> sortedWordCount = sortedSwapedWordCount.mapToPair(Tuple2::swap);

        System.out.println(sortedWordCount.collect());

        jsc.stop();

    }


}
