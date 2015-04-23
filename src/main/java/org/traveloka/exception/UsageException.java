package org.traveloka.exception;

/**
 * Created by ariesutiono on 22/04/15.
 */
public class UsageException extends Exception {
  private static final String usage = ", usage: \"java -jar <jarname> <topic separated by comma> <n thread listening>\"";
  public UsageException(String msg){
    super(msg + usage);
  }
}
