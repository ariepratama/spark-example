package org.traveloka.helper;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.List;

/**
 * Created by ariesutiono on 29/04/15.
 */
public class DebugUtility {
  private static final String tag = "[SPARK SAMPLE]";
  private static final Logger logger = Logger.getLogger(tag);
  private static final String stars = "***";
  private static final String NEW_LINE = "\n";

  private static String constructMsg(List<Tuple2> dataset, String tag){
    StringBuilder res = new StringBuilder();
    res.append("-------------------------------------------------------" + NEW_LINE);
    res.append("Number of Retrieved dataset: " + dataset.size() + NEW_LINE);
    res.append("-------------------------------------------------------" + NEW_LINE);
    for(Tuple2 datum: dataset) {
      String msg = "[" + tag + "] key is = " + datum._1() + " value is = ";
      if (byte[].class.toString().equals(datum._2().getClass().toString()))
        res.append(msg + new String((byte[]) datum._2()) + NEW_LINE);
      else {
        Object obj = datum._2();
//        if (String.class.equals(obj.getClass())) {
//          System.out.println(msg + datum._2());
//        }
        if(Tuple2.class.equals(obj.getClass())){
          Tuple2 objTuple = (Tuple2) obj;
          res.append(msg + "[" + objTuple._1().toString() + "," + objTuple._2().toString() + "]"  + NEW_LINE);
        }else{
          res.append(msg + datum._2() + NEW_LINE);
        }
      }
    }
    return res.toString();
  }
  public static void logRdd(JavaPairRDD rdd, String tag){
    List<Tuple2> dataset = rdd.collect();
//    logger.info("-------------------------------------------------------");
//    logger.info("Number of Retrieved dataset: " + dataset.size());
//    logger.info("-------------------------------------------------------");
//    for(Tuple2 datum: dataset) {
//      String msg = "[" + tag + "] key is = " + datum._1() + " value is = ";
//      if (byte[].class.toString().equals(datum._2().getClass().toString()))
//        logger.info(msg + new String((byte[]) datum._2()));
//      else {
//        Object obj = datum._2();
//        if (String.class.equals(obj.getClass())) {
//          logger.info(msg + datum._2());
//        }
//        else if(Tuple2.class.equals(obj.getClass())){
//          Tuple2 objTuple = (Tuple2) obj;
//          logger.info(msg + objTuple._1().toString() + objTuple._2().toString());
//        }
//      }
//    }
    logger.info(constructMsg(dataset, tag));

  }

  public static void logSomething(String msg){
    logger.info(stars + msg + stars);
  }

  public static void printRdd(JavaPairRDD rdd, String tag){
    List<Tuple2> dataset = rdd.collect();
//    System.out.println("-------------------------------------------------------");
//    System.out.println("Number of Retrieved dataset: " + dataset.size());
//    System.out.println("-------------------------------------------------------");
//    for(Tuple2 datum: dataset) {
//      String msg = "[" + tag + "] key is = " + datum._1() + " value is = ";
//      if (byte[].class.toString().equals(datum._2().getClass().toString()))
//        System.out.println(msg + new String((byte[]) datum._2()));
//      else {
//        Object obj = datum._2();
////        if (String.class.equals(obj.getClass())) {
////          System.out.println(msg + datum._2());
////        }
//        if(Tuple2.class.equals(obj.getClass())){
//          Tuple2 objTuple = (Tuple2) obj;
//          System.out.println(msg + "[" + objTuple._1().toString() + "," + objTuple._2().toString() + "]");
//        }else{
//          System.out.println(msg + datum._2());
//        }
//      }
//    }
    System.out.println(constructMsg(dataset, tag));
  }

  public static void printRdd(JavaRDD rdd, String tag){
    List<Object> dataset = rdd.collect();
    System.out.println("-------------------------------------------------------");
    System.out.println("Number of Retrieved dataset: " + dataset.size());
    System.out.println("-------------------------------------------------------");
    for(Object obj: dataset) {
      String msg = "[" + tag + "] " + " value is = " + obj.toString();
      System.out.println(msg);
    }

  }

  public static void printSomething(String msg){
    System.out.println(tag + stars + msg + stars);
  }
}
