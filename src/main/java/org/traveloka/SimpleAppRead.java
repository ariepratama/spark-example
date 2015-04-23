package org.traveloka;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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
    JavaPairRDD<String, byte[]> rdd = null;
    if (! readAsHadoopFile)
      rdd = sc.textFile("s3n://mongodwh/spark-backup/" + StreamEventBackup.dateString + "/" + topic + "/*").mapToPair(new PairFunction<String, String, byte[]>() {
        @Override
        public Tuple2<String, byte[]> call(String s) throws Exception {
          return new Tuple2<String, byte[]>(s, s.getBytes());
        }
      });
    else {
      JavaPairRDD<Text, BytesWritable> rddTemp = sc.sequenceFile("s3n://mongodwh/spark-backup/" + StreamEventBackup.dateString + "/" + topic + "/*",
//              SequenceFileInputFormat.class,
              Text.class,
              BytesWritable.class);
      rdd = rddTemp.mapToPair(new ConvertToNative());
    }

    List<Tuple2<String, byte[]>> data  = rdd.collect();
    logger.info("#############rdd size#############: " + data.size());
    for (Tuple2<String, byte[]> datum: data){
      logger.info("**********>>>>" + datum._1() + ":" + new String(datum._2()));
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
