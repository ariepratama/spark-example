package org.traveloka;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.traveloka.exception.NoTopicException;
import org.traveloka.helper.ArgValidationUtility;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
    AvroTrytoDecode trytoDecode;
    public AvroValueDecode(String accessId, String accessKey, String bucketName, String bucketKey) throws IOException {
      trytoDecode = new AvroTrytoDecode(accessId, accessKey, bucketName, bucketKey);
    }

    @Override
    public Tuple2<String, String> call(Tuple2<String, byte[]> stringBytesTuple2) throws Exception {
      return new Tuple2<String, String>(stringBytesTuple2._1(), trytoDecode.fromBytes(stringBytesTuple2._2()));
    }
  }

  private static class AvroValueOnlyDecode implements Function<byte[], String>{
//    String accessId;
//    String accessKey;
//    String bucketName;
//    String bucketKey;
    AvroTrytoDecode trytoDecode;
    public AvroValueOnlyDecode(String accessId, String accessKey, String bucketName, String bucketKey) throws IOException {
//      this.accessId = accessId;
//      this.accessKey = accessKey;
//      this.bucketName = bucketName;
//      this.bucketKey = bucketKey;
      trytoDecode = new AvroTrytoDecode(accessId, accessKey, bucketName, bucketKey);
    }
    @Override
    public String call(byte[] bytes) throws Exception {
      return trytoDecode.fromBytes(bytes);
    }
  }

  private static class AvroTrytoDecode implements Serializable{
    Schema sch;
    BinaryDecoder avroBinaryDecoder;
    GenericDatumReader<GenericRecord> avroEventReader;
    GenericRecord avroEvent;
    public AvroTrytoDecode(String accessId, String accessKey, String bucketName, String bucketKey) throws IOException {
      AmazonS3Client s3Client = new AmazonS3Client(new BasicAWSCredentials(accessId, accessKey));
      S3Object obj = s3Client.getObject(new GetObjectRequest(bucketName, bucketKey));
      sch = new Schema.Parser().parse(obj.getObjectContent());

    }

    public String fromBytes(byte[] bytes){
      String res = "null";
      avroEventReader = new GenericDatumReader<GenericRecord>(sch);
      avroBinaryDecoder = DecoderFactory.get().binaryDecoder(new ByteBufferInputStream(Lists.newArrayList(ByteBuffer.wrap(bytes))),
              avroBinaryDecoder);
      try {
        avroEvent = avroEventReader.read(avroEvent, avroBinaryDecoder);
        res = avroEvent.get("source").toString();
        System.out.println("decoded String to: " + res);
      } catch (IOException e) {
        e.printStackTrace();
      }
      return res;
    }
  }

  private static void logRdd(JavaPairRDD rdd, String tag){
    List<Tuple2> dataset = rdd.collect();
    logger.info("-------------------------------------------------------");
    logger.info("Number of Retrieved dataset: " + dataset.size());
    logger.info("-------------------------------------------------------");
    for(Tuple2 datum: dataset) {
      String msg = "[" + tag + "] key is= " + datum._1() + "value is= ";
      if (byte[].class.toString().equals(datum._2().getClass().toString()))
        logger.info(msg + new String((byte[]) datum._2()));
      else
        logger.info(msg + datum._2().toString());
    }

  }

  private static void printRdd(JavaPairRDD rdd, String tag){
    List<Tuple2> dataset = rdd.collect();
    System.out.println("-------------------------------------------------------");
    System.out.println("Number of Retrieved dataset: " + dataset.size());
    System.out.println("-------------------------------------------------------");
    for(Tuple2 datum: dataset) {
      String msg = "[" + tag + "] key is= " + datum._1() + "value is= ";
      if (byte[].class.toString().equals(datum._2().getClass().toString()))
        System.out.println(msg + new String((byte[]) datum._2()));
      else
        System.out.println(msg + datum._2());
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



    final String topic = args[0];
    final String accessId = args[1];
    final String secretKey = args[2];
    final String bucketName = args[3];
    final String bucketKey = args[4] + topic + ".avsc";
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
//      JavaPairRDD<Text, BytesWritable> rddTemp = sc.sequenceFile("s3n://mongodwh/spark-backup/" + StreamEventBackup.dateString + "/" + topic + "/*",
//              Text.class,
//              BytesWritable.class);
      JavaPairRDD<Text, BytesWritable> rddTemp = sc.sequenceFile("s3n://mongodwh/spark-backup/" + StreamEventBackup.dateString + "/" + topic + "/*",
              Text.class,
              BytesWritable.class);
      rdd = rddTemp.mapToPair(new ConvertToNative());
    }
//    printRdd(rdd, "COLLECTED");

//    AmazonS3Client s3Client = new AmazonS3Client(new BasicAWSCredentials(accessId, secretKey));
//    S3Object obj = s3Client.getObject(new GetObjectRequest(bucketName, bucketKey));

    final Schema sch = new Schema.Parser().parse(obj.getObjectContent());
//    JavaRDD<String> testRdd = rdd.sample(false, 5).map(new Function<Tuple2<String, byte[]>, String>() {
//
//      BinaryDecoder avroBinaryDecoder;
//      GenericDatumReader<GenericRecord> avroEventReader;
//      GenericRecord avroEvent;
//
//      @Override
//      public String call(Tuple2<String, byte[]> stringTuple2) throws Exception {
//
//        avroEventReader = new GenericDatumReader<GenericRecord>(sch);
//        avroBinaryDecoder = DecoderFactory.get().binaryDecoder(new ByteBufferInputStream(Lists.newArrayList(ByteBuffer.wrap(stringTuple2._2()))), avroBinaryDecoder);
//        avroEvent = avroEventReader.read(avroEvent, avroBinaryDecoder);
//
//        return avroEvent.get("source").toString();
//      }
//    });
//    List<String> list = testRdd.collect();
//    for(String s: list){
//      System.out.println(s);
//    }

    JavaPairRDD<String, byte[]> distinctRdd = rdd.distinct();
    printRdd(distinctRdd, "DISTINCT");

    JavaPairRDD<String, byte[]> sample1 = sc.parallelize(rdd.takeSample(false, 5)).mapToPair(new PairFunction<Tuple2<String, byte[]>, String, byte[]>() {
      @Override
      public Tuple2<String, byte[]> call(Tuple2<String, byte[]> stringTuple2) throws Exception {
        return stringTuple2;
      }
    });
    printRdd(sample1, "SAMPLING1");
    JavaPairRDD<String, byte[]> sample2 = sc.parallelize(rdd.takeSample(false, 5)).mapToPair(new PairFunction<Tuple2<String, byte[]>, String, byte[]>() {
      @Override
      public Tuple2<String, byte[]> call(Tuple2<String, byte[]> stringTuple2) throws Exception {
        return stringTuple2;
      }
    });
    printRdd(sample2, "SAMPLING2");

    List<Tuple2<String, byte[]>> t = new ArrayList<Tuple2<String, byte[]>>();
    t.add(new Tuple2<String, byte[]>("direct", "asdfasdf".getBytes()));
    JavaPairRDD<String, byte[]> tRdd = sc.parallelizePairs(t);

    List<Tuple2<String, byte[]>> t1 = new ArrayList<Tuple2<String, byte[]>>();
    t1.add(new Tuple2<String, byte[]>("adwords", "asdfasdf1".getBytes()));
    t1.add(new Tuple2<String, byte[]>("google", "asdfasdf2".getBytes()));
    t1.add(new Tuple2<String, byte[]>("halohalo", "asdfasdf3".getBytes()));
    t1.add(new Tuple2<String, byte[]>("halohalo", "asdfasdf4".getBytes()));
    JavaPairRDD<String, byte[]> t1Rdd = sc.parallelizePairs(t1);


    JavaPairRDD<String, byte[]> intersectedRdd = sample1.intersection(sample2);
    printRdd(intersectedRdd, "INTERSECTION");

    JavaPairRDD<String, byte[]> subtractRdd = sample1.subtractByKey(tRdd);
    printRdd(subtractRdd, "SUBTRACT");

    JavaPairRDD<String, byte[]> unionRdd = sample1.union(tRdd);
    printRdd(unionRdd, "UNION");

    JavaPairRDD leftJoinedRdd = t1Rdd.leftOuterJoin(unionRdd);
    printRdd(leftJoinedRdd, "LEFT JOIN");

    JavaPairRDD<String, String> test1 = sample1.mapToPair(new AvroValueDecode(accessId, secretKey, bucketName, bucketKey));
    printRdd(test1, "DECODE");

//    List<Tuple2<String, byte[]>> collected = rdd.collect();




//    printRdd(decodedRdd, "DECODED");

//    JavaPairRDD<String, byte[]> leftJoinedRdd = unionRdd.leftOuterJoin(t1Rdd);

//    List<byte[]> t = sample1.values().collect();
//    for (byte[] t1:t){
//      logger.info("the value: " + decoder.fromBytes(t1));
//    }
//    JavaPairRDD<String, String> decodedRdd = rdd.mapToPair(new AvroValueDecode());
//    logRdd(decodedRdd, "DECODED");



  }
}
