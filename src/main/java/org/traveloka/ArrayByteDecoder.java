package org.traveloka;

import kafka.serializer.Decoder;
import org.apache.commons.lang.ArrayUtils;
import java.util.Arrays;
import java.util.List;

/**
 * Created by ariesutiono on 14/04/15.
 */
public class ArrayByteDecoder implements Decoder<List<Byte>> {
  @Override
  public List<Byte> fromBytes(byte[] bytes) {
    return Arrays.asList(ArrayUtils.toObject(bytes));
  }
}
