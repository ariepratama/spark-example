package org.traveloka;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import org.apache.avro.Schema;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.mapreduce.AvroSequenceFileOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
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
import org.traveloka.helper.ArgValidationUtility;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ariesutiono on 22/04/15.
 */
public class StreamEventBackup {

  public static class ConvertToWritableTypes implements PairFunction<Tuple2<String, byte[]>, Text, BytesWritable> {
    public Tuple2<Text, BytesWritable> call(Tuple2<String, byte[]> record) {
      return new Tuple2(new Text(record._1), new BytesWritable(record._2));
    }
  }
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

    // optional params
    if (args.length < 3 || args[2] == null || args[2].isEmpty()){
      throw new Exception("fill argument 3 with boolean that indicates if the file should be saved as hadoop file");
    }

    if (ArgValidationUtility.validate(args, 4)){
      throw new Exception("fill argument 4 with access id");
    }

    if (ArgValidationUtility.validate(args, 5)){
      throw new Exception("fill argument 5 with secret key");
    }

    if (ArgValidationUtility.validate(args, 6)){
      throw new Exception("fill argument 6 with bucket name");
    }

    if (ArgValidationUtility.validate(args, 7)){
      throw new Exception("fill argument 7 with bucket key");
    }

    //process arguments
//    final String[] topics = args[0].split(",");




    final boolean saveAsHadoopFile = Boolean.parseBoolean(args[2]);
    final String topic = args[0];
    int nThreads = Integer.parseInt(args[1]);

    final String accessId = args[3];
    final String secretKey = args[4];
    final String bucketName = args[5];
    final String bucketKey = args[6] + topic + ".avsc";


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
    messagePair.foreachRDD(new Function<JavaPairRDD<String, byte[]>, Void>() {
      @Override
      public Void call(JavaPairRDD<String, byte[]> stringJavaPairRDD) throws Exception {

        if (stringJavaPairRDD.partitions().size() != 0) {
          logger.info("partition size is not zero");
          List<String> keys = stringJavaPairRDD.keys().collect();
          for (String key : keys) {
            logger.info("key|********|" + key);
          }
          String filepath = "s3n://mongodwh/spark-backup/" + dateString + "/" + topic + "/" + "/partition-" + System.currentTimeMillis();
          if (saveAsHadoopFile) {
            JavaPairRDD<Text, BytesWritable> writeable = stringJavaPairRDD.mapToPair(new ConvertToWritableTypes());
            logger.info("----------SAVING AS HADOOP FILE----------");
//            writeable.saveAsHadoopFile(filepath,
//                    Text.class,
//                    BytesWritable.class,
//                    AvroOutputFormat.class);
            AmazonS3Client s3Client = new AmazonS3Client(new BasicAWSCredentials(accessId, secretKey));
            S3Object obj = s3Client.getObject(new GetObjectRequest(bucketName, bucketKey));

            JobConf hadoopJobConf = new JobConf();
            AvroJob.setInputSchema(hadoopJobConf, Schema.create(Schema.Type.BYTES));
            AvroJob.setOutputSchema(hadoopJobConf, new Schema.Parser().parse(obj.getObjectContent()));

            writeable.saveAsNewAPIHadoopFile(filepath,
                    Text.class,
                    AvroKeyValue.class,
                    AvroKeyOutputFormat.class,
                    hadoopJobConf);



          } else {
            logger.info("----------SAVING AS TEXT FILE----------");
            stringJavaPairRDD.saveAsTextFile(filepath);
          }

        } else {
          logger.info("partition size is zero!");
        }
        return null;
      }
    });




    jscc.start();
    jscc.awaitTermination();

  }
}
