package org.traveloka;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.traveloka.exception.NoTopicException;
import org.traveloka.helper.ArgValidationUtility;
import scala.Tuple2;

import java.util.List;

/**
 * Created by ariesutiono on 22/04/15.
 */
public class SimpleAppRead {
  private static Logger logger = Logger.getLogger(SimpleAppRead.class);
  private static AvroDecoder decoder;
  // ----------------------------------------
  // UTILITY FUNCTIONS
  // ----------------------------------------
  private static class ConvertToNative implements PairFunction<Tuple2<Text, BytesWritable>, String, byte[]>{
    @Override
    public Tuple2<String, byte[]> call(Tuple2<Text, BytesWritable> textBytesWritableTuple2) throws Exception {
      return new Tuple2<String, byte[]>(textBytesWritableTuple2._1().toString(), textBytesWritableTuple2._2.getBytes());
    }
  }

  private static class AvroValueDecode implements PairFunction<Tuple2<String, byte[]>, String, String>{
    @Override
    public Tuple2<String, String> call(Tuple2<String, byte[]> stringBytesTuple2) throws Exception {
      return new Tuple2<String, String>(stringBytesTuple2._1(), decoder.fromBytes(stringBytesTuple2._2()));
    }
  }

  private static void logRdd(JavaPairRDD rdd, String tag){
    List<Tuple2> dataset = rdd.collect();
    logger.info("-------------------------------------------------------");
    logger.info("Number of Retrieved dataset: " + dataset.size());
    logger.info("-------------------------------------------------------");
    for(Tuple2 datum: dataset) {
      String msg = "[" + tag + "] key is=" + datum._1() + "value is=";
      if (datum._2().getClass().toString().equals(byte[].class.toString()))
        logger.info(msg + new String((byte[]) datum._2()));
      else
        logger.info(msg + datum._2().toString());
    }

  }


  public static void main(String args[]) throws Exception {
    SparkConf conf = new SparkConf().setAppName(SimpleAppRead.class.getSimpleName());

    if (ArgValidationUtility.validate(args, 1)){
      throw new NoTopicException("topic is empty");
    }

    if (ArgValidationUtility.validate(args, 2)){
      throw new Exception("No access id");
    }

    if (ArgValidationUtility.validate(args, 3)){
      throw new Exception("no secret key provided");
    }
    if (ArgValidationUtility.validate(args, 4)){
      throw new Exception("no bucket name provided");
    }

    if (ArgValidationUtility.validate(args, 4)){
      throw new Exception("no bucket key provided");
    }

    if (ArgValidationUtility.validate(args, 5)){
      throw new Exception("should the program read as hadoop file?");
    }

    // handle s3 schema
//    decoder = new AvroDecoder(SimpleAppRead.class.getResource("/schema/flight.visit.avsc").openStream());



    String topic = args[0];
    String accessId = args[1];
    String secretKey = args[2];
    String bucketName = args[3];
    String bucketKey = args[4] + topic + ".avsc";
    boolean readAsHadoopFile = Boolean.parseBoolean(args[5]);
    AmazonS3Client s3Client = new AmazonS3Client(new BasicAWSCredentials(accessId, secretKey));
    S3Object obj = s3Client.getObject(new GetObjectRequest(bucketName, bucketKey));
    decoder = new AvroDecoder(obj.getObjectContent());

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
    logRdd(intersectedRdd,"INTERSECTION");

    List<byte[]> t = rdd.values().collect();
    for (byte[] t1:t){
      logger.info("the value: " + decoder.fromBytes(t1));
    }
//    JavaPairRDD<String, String> decodedRdd = rdd.mapToPair(new AvroValueDecode());
//    logRdd(decodedRdd, "DECODED");



  }
}
