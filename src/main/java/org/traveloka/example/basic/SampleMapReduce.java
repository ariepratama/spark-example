package org.traveloka.example.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
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

    // reduce operation to perform min function by key
    reducedByKey = rdd1.reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer integer, Integer integer2) throws Exception {
        return Math.min(integer, integer2);
      }
    });

    DebugUtility.printRdd(reducedByKey, "RDD1-REDUCED-MIN");

    // unique latest operation

    List<Tuple2<String, Tuple2<Long, Integer>>> data1WithTime = new ArrayList<Tuple2<String, Tuple2<Long, Integer>>>();
    data1WithTime.add(new Tuple2<String, Tuple2<Long, Integer>>(KEYS[0], new Tuple2<Long, Integer>(System.currentTimeMillis(),VALUES[0])));
    data1WithTime.add(new Tuple2<String, Tuple2<Long, Integer>>(KEYS[0], new Tuple2<Long, Integer>(System.currentTimeMillis(),VALUES[1])));
    data1WithTime.add(new Tuple2<String, Tuple2<Long, Integer>>(KEYS[0], new Tuple2<Long, Integer>(System.currentTimeMillis(),VALUES[2])));
    data1WithTime.add(new Tuple2<String, Tuple2<Long, Integer>>(KEYS[1], new Tuple2<Long, Integer>(System.currentTimeMillis(),VALUES[2])));
    data1WithTime.add(new Tuple2<String, Tuple2<Long, Integer>>(KEYS[1], new Tuple2<Long, Integer>(System.currentTimeMillis(),VALUES[0])));
    data1WithTime.add(new Tuple2<String, Tuple2<Long, Integer>>(KEYS[1], new Tuple2<Long, Integer>(System.currentTimeMillis(),VALUES[1])));

    JavaPairRDD<String, Tuple2<Long, Integer>> rdd1WithTime = sc.parallelizePairs(data1WithTime);
    DebugUtility.printRdd(rdd1WithTime, "RDD1-TIME-CONSTRUCT");

    JavaPairRDD<String, Tuple2<Long, Integer>> reducedWithTime= rdd1WithTime.reduceByKey(new Function2<Tuple2<Long, Integer>, Tuple2<Long, Integer>, Tuple2<Long, Integer>>() {
      @Override
      public Tuple2<Long, Integer> call(Tuple2<Long, Integer> longIntegerTuple2, Tuple2<Long, Integer> longIntegerTuple22) throws Exception {
        return (longIntegerTuple2._1() >= longIntegerTuple22._1()) ? longIntegerTuple2 : longIntegerTuple22;
      }
    });


    JavaPairRDD<String, Integer> latest = reducedWithTime.mapToPair(new PairFunction<Tuple2<String, Tuple2<Long, Integer>>, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(Tuple2<String, Tuple2<Long, Integer>> stringTuple2Tuple2) throws Exception {
        return new Tuple2<String, Integer>(stringTuple2Tuple2._1(), stringTuple2Tuple2._2()._2());
      }
    });
    DebugUtility.printRdd(latest, "RDD1-TIME-LATEST");





  }
}
