package org.traveloka;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.joda.time.DateTime;
import org.traveloka.exception.NoThreadException;
import org.traveloka.exception.NoTopicException;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ariesutiono on 22/04/15.
 */
public class StreamEventBackup {

  // -----------------------------------
  // UTILITY CONFIG
  // -----------------------------------
  private static final Logger logger = Logger.getLogger(StreamEventBackup.class);
  public static final DateTime date = new DateTime(System.currentTimeMillis());
  public static final String dateString = date.getYear() + "-" + date.getMonthOfYear() +
          "-" + date.getDayOfMonth();

  // -----------------------------------
  // KAFKA CONFIG
  // -----------------------------------
  private static final String KAFKA_DEFAULT_GROUPID = "default-group";
  private static final String KAFKA_DEFAULT_ZOOKEEPER = "localhost:2181";

  // -----------------------------------
  // SPARK CONFIG
  // -----------------------------------
  private static final int SPARK_STREAMING_DEVIDE_INTERVAL = 10000;
  private static JavaPairReceiverInputDStream<String, String> kafkaStream;



  private StreamEventBackup(){}

  /**
   * args[0] is name of topics tobe listened
   * args[1] is number of thread used in listening the topics (same for all topics)
   * @param args
   */
  public static void main(String[] args) throws Exception {
    SparkConf conf = new SparkConf().setAppName(StreamEventBackup.class.getSimpleName());
    //check if correct params exists
    if (args.length < 1 || args[0] == null || args[0].isEmpty()){
      throw new NoTopicException("topic is empty");
    }

    if (args.length < 2 || args[1] == null || args[1].isEmpty()){
      throw new NoThreadException("number of threads is empty");
    }

    //process arguments
//    final String[] topics = args[0].split(",");
    final String topic = args[0];
    int nThreads = Integer.parseInt(args[1]);


    Map<String, Integer> topicMap = new HashMap<String, Integer>();
    topicMap.put(topic, nThreads);


    // initiate kafka and spark stream
    Map<String, String> kafkaParams = new HashMap<String, String>();
    kafkaParams.put("group.id", KAFKA_DEFAULT_GROUPID);
    kafkaParams.put("zookeeper.connect", KAFKA_DEFAULT_ZOOKEEPER);
    kafkaParams.put("zookeeper.session.timeout.ms", "3000");
    kafkaParams.put("zookeeper.sync.time.ms", "200");
    kafkaParams.put("auto.commit.interval.ms", (SPARK_STREAMING_DEVIDE_INTERVAL * 2) + "");
    kafkaParams.put("auto.commit.enable","true");


    JavaStreamingContext jscc = new JavaStreamingContext(conf, new Duration(SPARK_STREAMING_DEVIDE_INTERVAL));

    // make incoming message into bytes by DefaultDecoder
    JavaPairReceiverInputDStream<String, byte[]> messages  = KafkaUtils.createStream(jscc,
            String.class,
            byte[].class,
            StringDecoder.class,
            DefaultDecoder.class,
            kafkaParams,
            topicMap,
            StorageLevel.MEMORY_AND_DISK_SER_2());

    JavaPairDStream<String, byte[]> messagePair = messages.mapToPair(new PairFunction<Tuple2<String, byte[]>, String, byte[]>() {
      @Override
      public Tuple2<String, byte[]> call(Tuple2<String, byte[]> stringTuple2) throws Exception {
        return new Tuple2<String, byte[]>(topic, stringTuple2._2());
      }
    });

    logger.info("---------------------------------------------");
    logger.info("Save to Database if rdd is not empty");
    logger.info("---------------------------------------------");
//    messagePair.saveAsHadoopFiles("s3n://mongodwh/spark-backup/" + dateString + "/" + topic + "/" + "/partition-" + System.currentTimeMillis(),
//            "output",
//            NullWritable.class,
//            BytesWritable.class,
//            SequenceFileOutputFormat.class);

    messagePair.foreachRDD(new Function<JavaPairRDD<String, byte[]>, Void>() {
      @Override
      public Void call(JavaPairRDD<String, byte[]> stringJavaPairRDD) throws Exception {

        if (stringJavaPairRDD.partitions().size() != 0) {
          logger.info("partition size is not zero");
          List<String> keys = stringJavaPairRDD.keys().collect();
//          Iterator iter = keys.iterator();
          for (String key : keys){
            logger.info("key|********|" + key);
          }

//          List<byte[]> vals = stringJavaPairRDD.values().collect();

          //stringJavaPairRDD.saveAsTextFile("s3n://mongodwh/spark-backup/" + dateString + "/" + topics[0] + "/" + "/partition-" + System.currentTimeMillis());
//          stringJavaPairRDD.saveAsHadoopFile("s3n://mongodwh/spark-backup/" + dateString + "/" + topic + "/" + "/partition-" + System.currentTimeMillis(),
//                  NullWritable.class,
//                  BytesWritable.class,
//                  SequenceFileOutputFormat.class);
          stringJavaPairRDD.saveAsTextFile("s3n://mongodwh/spark-backup/" + dateString + "/" + topic + "/" + "/partition-" + System.currentTimeMillis());

        } else{
          logger.info("partition size is zero!");
        }
        return null;
      }
    });

    jscc.start();
    jscc.awaitTermination();

  }
}
