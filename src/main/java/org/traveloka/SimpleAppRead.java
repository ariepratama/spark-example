package org.traveloka;

import org.apache.hadoop.mapred.*;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.traveloka.exception.NoTopicException;
import java.util.List;

/**
 * Created by ariesutiono on 22/04/15.
 */
public class SimpleAppRead {
  private static Logger logger = Logger.getLogger(SimpleAppRead.class);

  public static void main(String args[]) throws Exception {
    SparkConf conf = new SparkConf().setAppName(SimpleAppRead.class.getSimpleName());
    if (args.length < 1 || args[0] == null || args[0].isEmpty()){
      throw new NoTopicException("topic is empty");
    }

//    if (args.length < 2 || args[0] == null || args[0].isEmpty()){
//      throw new Exception("argument 2 missing: fill with timestamp");
//    }

    String topic = args[0];
//    String timestamp = args[1];

    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> rdd = sc.textFile("s3n://mongodwh/spark-backup/" + StreamEventBackup.dateString + "/" + topic + "/*");
    List<String> data  = rdd.collect();
    for (String datum: data){
      logger.info("**********>>>>" + datum);
    }
//    JavaPairRDD<byte[], byte[]> data = sc.hadoopFile("s3n://spark-backup/" + StreamEventBackup.dateString + "/" + topic + "/*",
//            SequenceFileInputFormat.class,
//            byte[].class,
//            byte[].class);
//
//    List<byte[]> keys = data.values().collect();
//    for (byte[] key : keys){
//      String s = new String(key);
//      logger.info("the value is: " + s);
//    }

  }
}
