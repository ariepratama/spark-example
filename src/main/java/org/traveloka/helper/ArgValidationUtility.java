package org.traveloka.helper;

/**
 * Created by ariesutiono on 24/04/15.
 */
public class ArgValidationUtility {

  public static boolean validate(String[] args, int numberArgs){
    int argIndex = numberArgs - 1 ;
    return args.length < numberArgs || args[argIndex] == null || args[argIndex].isEmpty();
  }
}
