package com.yaoting117.learning.bigdata.sparkcore.day01.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class WordCount {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("JavaWordCount");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = jsc.textFile("G:\\0916\\big-data\\learning-spark\\input\\word.txt");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                String[] words = line.split(" ");

                /*Iterator<String> resultIterator = null;

                ArrayList<String> wordList = new ArrayList<>();
                for (String word : words) {
                    wordList.add(word);

                }
                resultIterator = wordList.iterator();

                resultIterator = Arrays.asList(words).iterator();*/

                return Arrays.stream(words).iterator();
            }
        });

        JavaPairRDD<String, Integer> wordWithOne = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return Tuple2.apply(word, 1);
            }
        });

        JavaPairRDD<String, Integer> reducedWordCount = wordWithOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaPairRDD<Integer, String> swapedWordCount = reducedWordCount.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.swap();
            }
        });

        JavaPairRDD<Integer, String> sortedSwapedWordCount = swapedWordCount.sortByKey(false);

        JavaPairRDD<String, Integer> sortedWordCount = sortedSwapedWordCount.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2.swap();
            }
        });

        System.out.println(sortedSwapedWordCount.collect());

        jsc.stop();

    }
}

