package org.traveloka.example.model;

import org.traveloka.helper.DebugUtility;
import scala.Serializable;


/**
 * Created by ariesutiono on 29/04/15.
 */
public class Visit implements Serializable {
  private String userId;
  private String source;
  private int timestamp;

  public Visit(String _userId, String _source, int _time){
    this.userId = _userId;
    this.source = _source;
    this.timestamp = _time;

  }

  /**
   * copy constructor
   * @param visit
   */
  public Visit(Visit visit){
    this.userId = visit.userId;
    this.source = visit.source;
    this.timestamp = visit.timestamp;
  }

  public String getUserId() {
    return userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public int getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(int timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public boolean equals(Object visit){
    DebugUtility.printSomething("calling equals on object");
    if (visit == null) {
      DebugUtility.printSomething("other visit is null");
      return false;
    }
    else{
      Visit visitObj = (Visit) visit;
      boolean res = (visitObj.timestamp == this.timestamp) &&
              ((visitObj.userId == null && this.userId == null) || (visitObj.userId != null && visitObj.userId.equals(this.userId))) &&
              ((visitObj.source == null && this.source == null) || (visitObj.source != null && visitObj.source.equals(this.source)));
      DebugUtility.printSomething("result is :" + res + " is userid same? " + visitObj.userId.equals(this.userId) + "is source same? " + visitObj.source.equals(this.source));
      return res;

    }

  }

  @Override
  public String toString(){
    return "[" + this.userId + "," + this.source + "," + this.timestamp + "]";
  }

  @Override
  public int hashCode(){
    return this.timestamp;
  }
}
