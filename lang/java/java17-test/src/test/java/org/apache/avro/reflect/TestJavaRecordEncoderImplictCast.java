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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;

import static org.apache.avro.reflect.RecordReadWriteUtil.read;
import static org.apache.avro.reflect.RecordReadWriteUtil.write;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestJavaRecordEncoderImplictCast {

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  public void testUp1(boolean genTypes) throws IOException {
    Base in = new Base(1D, 2F, 3L, 4, (short) 5, (byte) 6, (char) 7);
    byte[] encoded = write(genTypes, in, Up1.class);
    var expected = new Up1(1D, 2.0, 3L, 4L, (char) 5, (short) 6, (byte) 7);
    assertEquals(expected, read(genTypes, encoded));
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  public void testUp2(boolean genTypes) throws IOException {
    Base in = new Base(1D, 2F, 3L, 4, (short) 5, (byte) 6, (char) 7);
    byte[] encoded = write(genTypes, in, Up2.class);
    var expected = new Up2(1D, 2.0, 3L, 4L, (char) 5, (short) 6, (byte) 7);
    assertEquals(expected, read(genTypes, encoded));
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  public void testUp3(boolean genTypes) throws IOException {
    Base in = new Base(1D, 2F, 3L, 4, (short) 5, (byte) 6, (char) 7);
    byte[] encoded = write(genTypes, in, Up3.class);
    var expected = new Up3(1D, 2.0, 3L, 4L, (char) 5, (short) 6, (byte) 7);
    assertEquals(expected, read(genTypes, encoded));
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  public void testSide1(boolean genTypes) throws IOException {
    Base in = new Base(1D, 2F, 3L, 4, (short) 5, (byte) 6, (char) 7);
    byte[] encoded = write(genTypes, in, Side1.class);
    var expected = new Side1(1D, 2.0F, 3L, 4L, (char) 5, (short) 6, (byte) 7);
    assertEquals(expected, read(genTypes, encoded));
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  public void testSide2(boolean genTypes) throws IOException {
    Base in = new Base(1D, 2F, 3L, 4, (short) 5, (byte) 6, (char) 7);
    byte[] encoded = write(genTypes, in, Side2.class);
    var expected = new Side2(1D, 2.0F, 3L, 4L, (char) 5, (short) 6, (byte) 7);
    assertEquals(expected, read(genTypes, encoded));
  }

  public record Base(double field1, float field2, long field3, int field4, short field5, byte field6, char field7) {
  }

  public record Up1(double field1, double field2, long field3, long field4, int field5, short field6, int field7) {
  }

  public record Up2(double field1, double field2, long field3, long field4, long field5, int field6, int field7) {
  }

  public record Up3(double field1, double field2, long field3, long field4, long field5, long field6, long field7) {
  }

  public record Side1(double field1, float field2, double field3, float field4, float field5, float field6,
      float field7) {
  }

  public record Side2(double field1, float field2, double field3, double field4, double field5, double field6,
      double field7) {
  }

}
