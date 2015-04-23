package org.traveloka;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.traveloka.exception.NoTopicException;
import scala.Tuple2;

import java.util.List;

/**
 * Created by ariesutiono on 22/04/15.
 */
public class SimpleAppRead {
  private static Logger logger = Logger.getLogger(SimpleAppRead.class);

  private static class ConvertToNative implements PairFunction<Tuple2<Text, BytesWritable>, String, byte[]>{

    @Override
    public Tuple2<String, byte[]> call(Tuple2<Text, BytesWritable> textBytesWritableTuple2) throws Exception {
      return new Tuple2<String, byte[]>(textBytesWritableTuple2._1().toString(), textBytesWritableTuple2._2.getBytes());
    }
  }



  private static void logRdd(JavaPairRDD<String, byte[]> rdd, String tag){
    List<Tuple2<String, byte[]>> dataset = rdd.collect();
    logger.info("-------------------------------------------------------");
    logger.info("Number of Retrieved dataset: " + dataset.size());
    logger.info("-------------------------------------------------------");
    for(Tuple2<String, byte[]> datum: dataset)
      logger.info("["+ tag + "] key is=" + datum._1() + "value is=" + new String(datum._2()));
  }

  public static void main(String args[]) throws Exception {
    SparkConf conf = new SparkConf().setAppName(SimpleAppRead.class.getSimpleName());
    if (args.length < 1 || args[0] == null || args[0].isEmpty()){
      throw new NoTopicException("topic is empty");
    }

//    if (args.length < 2 || args[0] == null || args[0].isEmpty()){
//      throw new Exception("argument 2 missing: fill with timestamp");
//    }
    boolean readAsHadoopFile = false;

    // optional params
    if (args.length >= 2 && (args[1] != null || ! args[1].isEmpty())){
      readAsHadoopFile = Boolean.parseBoolean(args[1]);
    }
    String topic = args[0];
//    String timestamp = args[1];

    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaPairRDD<String, byte[]> rdd;
    if (! readAsHadoopFile)
      rdd = sc.textFile("s3n://mongodwh/spark-backup/" + StreamEventBackup.dateString + "/" + topic + "/*").mapToPair(new PairFunction<String, String, byte[]>() {
        @Override
        public Tuple2<String, byte[]> call(String s) throws Exception {
          return new Tuple2<String, byte[]>(s, s.getBytes());
        }
      });
    else {
      JavaPairRDD<Text, BytesWritable> rddTemp = sc.sequenceFile("s3n://mongodwh/spark-backup/" + StreamEventBackup.dateString + "/" + topic + "/*",
              Text.class,
              BytesWritable.class);
      rdd = rddTemp.mapToPair(new ConvertToNative());
    }
    logRdd(rdd,"COLLECTED");

    JavaPairRDD<String, byte[]> distinctRdd = rdd.distinct();
    logRdd(distinctRdd,"DISTINCT");

    JavaPairRDD<String, byte[]> sample1 = sc.parallelize(rdd.takeSample(false, 5)).mapToPair(new PairFunction<Tuple2<String, byte[]>, String, byte[]>() {
      @Override
      public Tuple2<String, byte[]> call(Tuple2<String, byte[]> stringTuple2) throws Exception {
        return stringTuple2;
      }
    });
    logRdd(sample1,"SAMPLING");

    JavaPairRDD<String, byte[]> intersectedRdd = rdd.intersection(sample1);
    logRdd(sample1,"INTERSECTION");

  }
}
