package org.traveloka;

import kafka.serializer.StringDecoder;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.joda.time.DateTime;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by ariesutiono on 13/04/15.
 */
public class SimpleApp {
  private static final Pattern SPACE = Pattern.compile(" ");
  private static JavaPairReceiverInputDStream<String, String> kafkaStream;
  private static final Logger logger = Logger.getLogger(SimpleApp.class);
  private static final DateTime date = new DateTime(System.currentTimeMillis());
  private static final String dateString = date.getYear() + "-" + date.getMonthOfYear() +
          "-" + date.getDayOfMonth();


  private SimpleApp(){}

  public static void main(String args[]) throws Exception {
    SparkConf conf = new SparkConf().setAppName("SimpleApp");

    String topic = "flight.visit";
    String groupId = "visit-group";

    Map<String, String> kafkaParams = new HashMap<String, String>();
    kafkaParams.put("group.id", groupId);
    kafkaParams.put("zookeeper.connect", "localhost:2181");
    kafkaParams.put("zookeeper.session.timeout.ms", "3000");
    kafkaParams.put("zookeeper.sync.time.ms", "200");
    kafkaParams.put("auto.commit.interval.ms", "60000");
    kafkaParams.put("auto.commit.enable","false");


    //create topic listener
    int numThreads = 3;
    Map<String, Integer> topicMap = new HashMap<String, Integer>();
    topicMap.put(topic,numThreads);

    JavaStreamingContext jscc = new JavaStreamingContext(conf, new Duration(10000));


//    JavaPairReceiverInputDStream<String, String> messages  = KafkaUtils.createStream(jscc,
//            "10.10.5.4:2181",
//            groupId,
//            topicMap);
    JavaPairReceiverInputDStream<String, String> messages  = KafkaUtils.createStream(jscc,
            String.class,
            String.class,
            StringDecoder.class,
            AvroDecoder.class,
            kafkaParams,
            topicMap,
            StorageLevel.MEMORY_AND_DISK_SER_2());



//    JavaDStream<String> stream = messages.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, String>>, String>() {
//      @Override
//      public Iterable<String> call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
//        InputStream schemaFile = SimpleApp.class.getResource("/schema/pageview.avsc").openStream();
//        logger.info("schema file available? " + schemaFile.available());
//        Schema.Parser parser = new Schema.Parser();
//        Schema sch = parser.parse(schemaFile);
//        DecoderFactory avroDecoderFactory = DecoderFactory.get();
//        GenericDatumReader<GenericRecord> avroEventReader = new GenericDatumReader<GenericRecord>(sch);
//        BinaryDecoder avroBinaryDecoder = null;
//        GenericRecord avroEvent = null;
//
//        List<String> result = new ArrayList<String>();
//
//        while(tuple2Iterator.hasNext()) {
//          Tuple2<String, String> tuple2 = (Tuple2<String, String>) tuple2Iterator.next();
//
//          byte[] event = Arrays.copyOfRange(event1, 0, event1.length-2);
//          InputStream kafkaMessageInputStream = new ByteBufferInputStream(Lists.newArrayList(ByteBuffer.wrap(event)));
//          avroBinaryDecoder = avroDecoderFactory.binaryDecoder(kafkaMessageInputStream, avroBinaryDecoder);
//          if (avroBinaryDecoder == null)
//            logger.error("avroBinaryDecoder is null !!!!!!!!!!!");
//          avroEvent = avroEventReader.read(avroEvent, avroBinaryDecoder);
//
//          System.out.println(avroEvent);
//          result.add((avroEvent != null) ? "avro event is null!" : avroEvent.toString());
//        }
//        return result;
//      }
//    });
//    stream.print(100);
//    stream.print();

//    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
//
//      @Override
//      public String call(Tuple2<String, String> tuple2) throws Exception {
//        InputStream schemaFile = SimpleApp.class.getResource("/schema/pageview.avsc").openStream();
//        logger.info("schema file available? " + schemaFile.available());
//        Schema.Parser parser = new Schema.Parser();
//        Schema sch = parser.parse(schemaFile);
//        DecoderFactory avroDecoderFactory = DecoderFactory.get();
//        GenericDatumReader<GenericRecord> avroEventReader = new GenericDatumReader<GenericRecord>(sch);
//        BinaryDecoder avroBinaryDecoder = null;
//        GenericRecord avroEvent = null;
//
//        byte[] event1 = tuple2._2().getBytes();
//        byte[] event = Arrays.copyOfRange(event1, 0, event1.length-2);
//        InputStream kafkaMessageInputStream = new ByteBufferInputStream(Lists.newArrayList(ByteBuffer.wrap(event)));
//        avroBinaryDecoder = avroDecoderFactory.binaryDecoder(kafkaMessageInputStream, avroBinaryDecoder);
//        if (avroBinaryDecoder == null)
//          logger.error("avroBinaryDecoder is null !!!!!!!!!!!");
//        avroEvent = avroEventReader.read(avroEvent, avroBinaryDecoder);
//
//        System.out.println(avroEvent);
//        return (avroEvent != null) ? "avro event is null!" : avroEvent.toString();
//      }
//    });
//    lines.print();

    JavaDStream<String> lines = messages.map(
        new Function<Tuple2<String, String>, String>() {
           @Override
           public String call(Tuple2<String, String> stringStringTuple2) throws Exception {
             return stringStringTuple2._2();
           }
         });

    JavaPairDStream<String, String> linespair = lines.mapToPair(new PairFunction<String, String, String>() {
      @Override
      public Tuple2<String, String> call(String s) throws Exception {
        return new Tuple2<String, String>(s, s);
      }
    });
//    logger.info("RDD counts: " + linespair.dstream().);
    logger.info("---------------------------------------------");
    logger.info("Save to Database if rdd is not empty");
    logger.info("---------------------------------------------");
    lines.foreachRDD(new Function<JavaRDD<String>, Void>() {
      @Override
      public Void call(JavaRDD<String> stringJavaRDD) throws Exception {
        //
        if (stringJavaRDD.partitions().size() != 0) {
          logger.info("partition size is not zero");
          stringJavaRDD.saveAsTextFile("s3n://mongodwh/spark-backup/" + dateString + "/partition-" + System.currentTimeMillis());
        } else{
          logger.info("partition size is zero!");
        }
        return null;
      }
    });

    

//    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//      @Override
//      public Iterable<String> call(String x) {
//        return Lists.newArrayList(SPACE.split(x));
//      }
//    });
//    InputStream kafkaMessageInputStream = null;
//    BinaryDecoder avroBinaryDecoder = null;
//    GenericRecord avroEvent = null;
//    try {
//      kafkaMessageInputStream = new ByteBufferInputStream(Lists.newArrayList(ByteBuffer.wrap(it.next().message())));
//      avroBinaryDecoder = avroDecoderFactory.binaryDecoder(kafkaMessageInputStream, avroBinaryDecoder);
//      avroEvent = avroEventReader.read(avroEvent, avroBinaryDecoder);
//      System.out.println("Thread: " + m_threadNumber);
//      System.out.println(avroEvent);
//      kafkaMessageInputStream.close();
//    } catch (Exception ex) {
//      System.out.println("Unable to process event from kafka, see inner exception details" + ex);
//      ex.printStackTrace();
//    }
//    JavaPairDStream<String, Integer> wordCounts = lines.mapToPair(
//      new PairFunction<String, String, Integer>() {
//        @Override
//        public Tuple2<String, Integer> call(String s) {
//          return new Tuple2<String, Integer>(s, 1);
//        }
//      }).reduceByKey(new Function2<Integer, Integer, Integer>() {
//      @Override
//      public Integer call(Integer i1, Integer i2) {
//        return i1 + i2;
//      }
//    });

//    wordCounts.print();
    jscc.start();
    jscc.awaitTermination();

  }

}
