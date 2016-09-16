package org.pb;

import java.util.Arrays;
import java.util.List;
import java.lang.Iterable;

import scala.Tuple2;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;


public class WordCount {
  public static void main(String[] args) throws Exception {
    String inputFile = args[0];
    String outputFile = "op";
    System.setProperty("hadoop.home.dir", "C:\\winutil\\");

    // Create a Java Spark Context.
    SparkConf sparkConf = new SparkConf().setMaster("local[2]")
    		.setAppName("Wordcount").set("spark.ui.port", "4567" )
    		.set("spark.driver.allowMultipleContexts", "true");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    // Load our input data.
    JavaRDD<String> input = sc.textFile(inputFile);
    // Split up into words.
    JavaRDD<String> words = input.flatMap(
      new FlatMapFunction<String, String>() {
        public Iterable<String> call(String x) {
          return Arrays.asList(x.split(" "));
        }});
    // Transform into word and count.
    JavaPairRDD<String, Integer> counts = words.mapToPair(
      new PairFunction<String, String, Integer>(){
        public Tuple2<String, Integer> call(String x){
          return new Tuple2(x, 1);
        }}).reduceByKey(new Function2<Integer, Integer, Integer>(){
            public Integer call(Integer x, Integer y){ return x + y;}});
    // Save the word count back out to a text file, causing evaluation.
    
    /*List<Tuple2<String, Integer>> output = counts.collect();
    for(Tuple2<?,?> tuple: output) {
    System.out.println(tuple._1()+": "+ tuple._2());
    }*/
    counts.saveAsTextFile(outputFile);
	}
}
