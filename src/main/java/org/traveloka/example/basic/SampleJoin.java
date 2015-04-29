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
  private static final int[] KEYS_INT = new int[]{1,2,3};


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
    DebugUtility.printRdd(rdd1, "RDD1-STRING");

    //construct rdd 2
    List<Tuple2<String, Integer>> data2 = new ArrayList<Tuple2<String, Integer>>();
    data2.add(new Tuple2<String, Integer>(KEYS[1], 100));
    data2.add(new Tuple2<String, Integer>(KEYS[1], 100));
    data2.add(new Tuple2<String, Integer>(KEYS[1], 100));
    data2.add(new Tuple2<String, Integer>(KEYS[1], 100));
    data2.add(new Tuple2<String, Integer>(KEYS[2], 100));

    JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(data2);
    DebugUtility.logSomething("finished parallelized rdd2");
    DebugUtility.printRdd(rdd1, "RDD2-STRING");

    DebugUtility.logSomething("------------- BEGIN JOIN -------------");
    JavaPairRDD<String, Tuple2<Integer, Integer>> rddJoin = rdd1.join(rdd2);
    DebugUtility.printRdd(rddJoin, "JOIN");

    DebugUtility.logSomething("------------- TRY WITH INTEGER -------------");
    //construct rdd 1
    List<Tuple2<Integer, Integer>> data1int = new ArrayList<Tuple2<Integer, Integer>>();
    data1int.add(new Tuple2<Integer, Integer>(KEYS_INT[0], 100));
    data1int.add(new Tuple2<Integer, Integer>(KEYS_INT[0], 100));
    data1int.add(new Tuple2<Integer, Integer>(KEYS_INT[2], 100));
    data1int.add(new Tuple2<Integer, Integer>(KEYS_INT[0], 100));
    data1int.add(new Tuple2<Integer, Integer>(KEYS_INT[1], 100));

    JavaPairRDD<Integer, Integer> rdd1int = sc.parallelizePairs(data1int);
    DebugUtility.logSomething("finished parallelized rdd1");
    DebugUtility.printRdd(rdd1int, "RDD1-STRING");

    //construct rdd 2
    List<Tuple2<Integer, Integer>> data2int = new ArrayList<Tuple2<Integer, Integer>>();
    data2int.add(new Tuple2<Integer, Integer>(KEYS_INT[1], 100));
    data2int.add(new Tuple2<Integer, Integer>(KEYS_INT[1], 100));
    data2int.add(new Tuple2<Integer, Integer>(KEYS_INT[1], 100));
    data2int.add(new Tuple2<Integer, Integer>(KEYS_INT[1], 100));
    data2int.add(new Tuple2<Integer, Integer>(KEYS_INT[2], 100));

    JavaPairRDD<Integer, Integer> rdd2int = sc.parallelizePairs(data2int);
    DebugUtility.logSomething("finished parallelized rdd2");
    DebugUtility.printRdd(rdd1, "RDD2-STRING");

    DebugUtility.logSomething("------------- BEGIN JOIN -------------");
    JavaPairRDD<Integer, Tuple2<Integer, Integer>> rddJoinint = rdd1int.join(rdd2int);
    DebugUtility.printRdd(rddJoinint, "JOIN");







  }
}
