/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro.reflect;

import static org.apache.avro.reflect.RecordReadWriteUtil.read;
import static org.apache.avro.reflect.RecordReadWriteUtil.write;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestJavaRecordAndCustomEncoder {

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  public void testRead(boolean genTypes) throws IOException {
    var in = new CustomReadWrapper(new CustomRead("hello world"));
    byte[] encoded = write(genTypes, in);
    CustomReadWrapper decoded = read(genTypes, encoded);

    assertNotNull(decoded);
    assertEquals("Fixed", decoded.field().getField());
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  public void testWrite(boolean genTypes) throws IOException {
    var in = new CustomWriteWrapper(new CustomWrite("hello world"));
    byte[] encoded = write(genTypes, in);
    CustomWriteWrapper decoded = read(genTypes, encoded);

    assertNotNull(decoded);
    assertEquals("Override", decoded.field().getField());
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  public void testWriteFiedEncoder(boolean genTypes) throws IOException {
    var in = new CustomWriteWrapperWithEncoder(new CustomWrite("hello world"));
    byte[] encoded = write(genTypes, in);
    CustomWriteWrapperWithEncoder decoded = read(genTypes, encoded);

    assertNotNull(decoded);
    assertEquals("Override2", decoded.field().getField());
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  public void testReadFiedEncoder(boolean genTypes) throws IOException {
    var in = new CustomReadWrapperWithEncoder(new CustomRead("hello world"));
    byte[] encoded = write(genTypes, in);
    CustomReadWrapperWithEncoder decoded = read(genTypes, encoded);

    assertNotNull(decoded);
    assertEquals("Fixed2", decoded.field().getField());
  }

  @AvroEncode(using = CustomEncoderWrite.class)
  public static class CustomWrite {

    private final String field;

    public CustomWrite(String field) {
      this.field = field;
    }

    public String getField() {
      return field;
    }
  }

  @AvroEncode(using = CustomEncoderRead.class)
  public static class CustomRead {

    private final String field;

    public CustomRead(String field) {
      this.field = field;
    }

    public String getField() {
      return field;
    }
  }

  public static class Custom {

    private final String field;

    public Custom(String field) {
      this.field = field;
    }

    public String getField() {
      return field;
    }
  }

  public static record CustomReadWrapper(CustomRead field) {
  }

  public static record CustomWriteWrapper(CustomWrite field) {
  }

  public static record CustomReadWrapperWithEncoder(@AvroEncode(using = CustomEncoderRead2.class) CustomRead field) {
  }

  public static record CustomWriteWrapperWithEncoder(@AvroEncode(using = CustomEncoderWrite2.class) CustomWrite field) {
  }

  public static class CustomEncoderRead extends CustomEncoding<CustomRead> {

    {
      schema = Schema.createRecord("CustomRead", null, "org.apache.avro.reflect.TestJavaRecordAndCustomEncoder", false,
          Arrays.asList(new Schema.Field("field", Schema.create(Schema.Type.STRING), null, null)));
    }

    @Override
    protected void write(Object datum, Encoder out) throws IOException {
      CustomRead c = (CustomRead) datum;
      out.writeString(c.getField());

    }

    @Override
    protected CustomRead read(Object reuse, Decoder in) throws IOException {
      in.readString();
      return new CustomRead("Fixed");
    }
  }

  public static class CustomEncoderRead2 extends CustomEncoding<CustomRead> {

    {
      schema = Schema.createRecord("CustomRead", null, "org.apache.avro.reflect.TestJavaRecordAndCustomEncoder", false,
          Arrays.asList(new Schema.Field("field", Schema.create(Schema.Type.STRING), null, null)));
    }

    @Override
    protected void write(Object datum, Encoder out) throws IOException {
      CustomRead c = (CustomRead) datum;
      out.writeString(c.getField());

    }

    @Override
    protected CustomRead read(Object reuse, Decoder in) throws IOException {
      in.readString();
      return new CustomRead("Fixed2");
    }
  }

  public static class CustomEncoderWrite extends CustomEncoding<CustomWrite> {

    {
      schema = Schema.createRecord("CustomWrite", null, "org.apache.avro.reflect.TestJavaRecordAndCustomEncoder", false,
          Arrays.asList(new Schema.Field("field", Schema.create(Schema.Type.STRING), null, null)));
    }

    @Override
    protected void write(Object datum, Encoder out) throws IOException {
      out.writeString("Override");
    }

    @Override
    protected CustomWrite read(Object reuse, Decoder in) throws IOException {
      return new CustomWrite(in.readString());
    }
  }

  public static class CustomEncoderWrite2 extends CustomEncoding<CustomWrite> {

    {
      schema = Schema.createRecord("CustomWrite", null, "org.apache.avro.reflect.TestJavaRecordAndCustomEncoder", false,
          Arrays.asList(new Schema.Field("field", Schema.create(Schema.Type.STRING), null, null)));
    }

    @Override
    protected void write(Object datum, Encoder out) throws IOException {
      out.writeString("Override2");
    }

    @Override
    protected CustomWrite read(Object reuse, Decoder in) throws IOException {
      return new CustomWrite(in.readString());
    }
  }
}
