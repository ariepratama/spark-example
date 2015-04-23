package org.traveloka;

import kafka.serializer.Decoder;
import org.apache.hadoop.io.Text;

/**
 * Created by ariesutiono on 22/04/15.
 */
public class HadoopTextDecoder implements Decoder<Text> {
  @Override
  public Text fromBytes(byte[] bytes) {
    return new Text(bytes);
  }
}
