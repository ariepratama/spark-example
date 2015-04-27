package org.traveloka;

import com.google.common.collect.Lists;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.ByteBufferInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Created by ariesutiono on 14/04/15.
 */
public class AvroDecoder implements Decoder<String>, Serializable {
  Schema sch;
  DecoderFactory avroDecoderFactory;
  BinaryDecoder avroBinaryDecoder;
  GenericDatumReader<GenericRecord> avroEventReader;
  GenericRecord avroEvent;
  public AvroDecoder() throws Exception{
//    this(null);

    InputStream ins = getClass().getResource("/schema/pageview.avsc").openStream();
    sch = new Schema.Parser().parse(ins);
    avroDecoderFactory = DecoderFactory.get();
    avroEventReader = new GenericDatumReader<GenericRecord>(sch);
  }

  public AvroDecoder(VerifiableProperties v) throws Exception{
    this();

  }

  public AvroDecoder(InputStream ins) throws Exception{
    if (ins == null){
      ins = getClass().getResource("/schema/pageview.avsc").openStream();
    }
    sch = new Schema.Parser().parse(ins);
    avroDecoderFactory = DecoderFactory.get();
    avroEventReader = new GenericDatumReader<GenericRecord>(sch);
  }
  @Override
  public String fromBytes(byte[] bytes) {
    InputStream kafkaMessageInputStream = new ByteBufferInputStream(Lists.newArrayList(ByteBuffer.wrap(bytes)));
    avroBinaryDecoder = avroDecoderFactory.binaryDecoder(kafkaMessageInputStream, avroBinaryDecoder);
    String res = "null";
    try {
      avroEvent = avroEventReader.read(avroEvent, avroBinaryDecoder);
      res = avroEvent.toString();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return res;
  }
}
