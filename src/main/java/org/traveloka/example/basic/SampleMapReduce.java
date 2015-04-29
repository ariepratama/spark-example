package org.traveloka.example.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.traveloka.helper.DebugUtility;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ariesutiono on 29/04/15.
 */
public class SampleMapReduce {

  private static final String[] KEYS = new String[] {"key1@mail","key2@mail","key3@mail"};
  private static final int[] VALUES = new int[] {1,2,3,4,5};

  public static void main(String[] args){
    SparkConf conf = new SparkConf().setAppName(SampleJoin.class.getSimpleName());
    JavaSparkContext sc = new JavaSparkContext(conf);

    List<Tuple2<String, Integer>> data1 = new ArrayList<Tuple2<String, Integer>>();
    data1.add(new Tuple2<String, Integer>(KEYS[0], VALUES[0]));
    data1.add(new Tuple2<String, Integer>(KEYS[0], VALUES[0]));
    data1.add(new Tuple2<String, Integer>(KEYS[1], VALUES[1]));
    data1.add(new Tuple2<String, Integer>(KEYS[1], VALUES[1]));
    data1.add(new Tuple2<String, Integer>(KEYS[2], VALUES[2]));
    data1.add(new Tuple2<String, Integer>(KEYS[2], VALUES[3]));
    JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data1);
    DebugUtility.printRdd(rdd1, "RDD1-CONSTRUCT");

    // grouped by key and sum
    JavaPairRDD<String, Integer> reducedByKey = rdd1.reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer integer, Integer integer2) throws Exception {
        return integer + integer2;
      }
    });
    DebugUtility.printRdd(reducedByKey, "RDD1-REDUCED-SUM");

    // add some more data
    data1.add(new Tuple2<String, Integer>(KEYS[0], VALUES[4]));
    data1.add(new Tuple2<String, Integer>(KEYS[1], VALUES[4]));
    data1.add(new Tuple2<String, Integer>(KEYS[2], VALUES[4]));

    rdd1 = sc.parallelizePairs(data1);
    DebugUtility.printRdd(rdd1, "RDD1-CONSTRUCT-2");

    // reduce operation to perform max function by key

    reducedByKey = rdd1.reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer integer, Integer integer2) throws Exception {
        return Math.max(integer, integer2);
      }
    });

    DebugUtility.printRdd(reducedByKey, "RDD1-REDUCED-MAX");



  }
}
