package org.traveloka.example.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.traveloka.helper.DebugUtility;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ariesutiono on 29/04/15.
 */
public class SampleJoin {
  private static final String[] KEYS = new String[]{"google", "bing", "direct"};


  public static void main (String[] args){
    SparkConf conf = new SparkConf().setAppName(SampleJoin.class.getSimpleName());
    JavaSparkContext sc = new JavaSparkContext(conf);

    //construct rdd 1
    List<Tuple2<String, Integer>> data1 = new ArrayList<Tuple2<String, Integer>>();
    data1.add(new Tuple2<String, Integer>(KEYS[0], 100));
    data1.add(new Tuple2<String, Integer>(KEYS[0], 100));
    data1.add(new Tuple2<String, Integer>(KEYS[2], 100));
    data1.add(new Tuple2<String, Integer>(KEYS[0], 100));
    data1.add(new Tuple2<String, Integer>(KEYS[1], 100));

    JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data1);
    DebugUtility.logSomething("finished parallelized rdd1");

    //construct rdd 2
    List<Tuple2<String, Integer>> data2 = new ArrayList<Tuple2<String, Integer>>();
    data1.add(new Tuple2<String, Integer>(KEYS[1], 100));
    data1.add(new Tuple2<String, Integer>(KEYS[1], 100));
    data1.add(new Tuple2<String, Integer>(KEYS[1], 100));
    data1.add(new Tuple2<String, Integer>(KEYS[1], 100));
    data1.add(new Tuple2<String, Integer>(KEYS[2], 100));

    JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(data2);
    DebugUtility.logSomething("finished parallelized rdd2");

    DebugUtility.logSomething("------------- BEGIN JOIN -------------");
    JavaPairRDD<String, Tuple2<Integer, Integer>> rddJoin = rdd1.join(rdd2);
    DebugUtility.printRdd(rddJoin, "JOIN");





  }
}
