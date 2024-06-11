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
import java.util.Arrays;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

import static org.apache.avro.reflect.RecordReadWriteUtil.read;
import static org.apache.avro.reflect.RecordReadWriteUtil.write;

public class TestJavaRecordEncoderFastReaders {

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  public void testBoolean(boolean genTypes) throws IOException {
    var in = new BooleanTypes(true, false);
    byte[] encoded = write(genTypes, in);
    BooleanTypes out = read(genTypes, encoded);
    assertEquals(in, out);
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  public void testBytes(boolean genTypes) throws IOException {
    byte first = 1;
    byte second = 2;
    var in = new ByteTypes(first, second);
    byte[] encoded = write(genTypes, in);
    ByteTypes out = read(genTypes, encoded);
    assertEquals(in, out);
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  public void testCharacter(boolean genTypes) throws IOException {
    var in = new CharacterTypes('a', 'B');
    byte[] encoded = write(genTypes, in);
    CharacterTypes out = read(genTypes, encoded);
    assertEquals(in, out);
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  public void testInteger(boolean genTypes) throws IOException {
    var in = new IntegerTypes(1, 2);
    byte[] encoded = write(genTypes, in);
    IntegerTypes out = read(genTypes, encoded);
    assertEquals(in, out);
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  public void testDouble(boolean genTypes) throws IOException {
    var in = new DoubleTypes(1D, 2D);
    byte[] encoded = write(genTypes, in);
    DoubleTypes out = read(genTypes, encoded);
    assertEquals(in, out);
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  public void testShort(boolean genTypes) throws IOException {
    short first = 1;
    short second = 2;
    var in = new ShortTypes(first, second);
    byte[] encoded = write(genTypes, in);
    ShortTypes out = read(genTypes, encoded);
    assertEquals(in, out);
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  public void testLong(boolean genTypes) throws IOException {
    var in = new LongTypes(1L, 2L);
    byte[] encoded = write(genTypes, in);
    LongTypes out = read(genTypes, encoded);
    assertEquals(in, out);
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  public void testFloat(boolean genTypes) throws IOException {
    var in = new FloatTypes(1F, 2F);
    byte[] encoded = write(genTypes, in);
    FloatTypes out = read(genTypes, encoded);
    assertEquals(in, out);
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  public void testObjects(boolean genTypes) throws IOException {
    var in = new ObjectType("String", new byte[] { 1, 2, 3 });
    byte[] encoded = write(genTypes, in);
    ObjectType out = read(genTypes, encoded);
    assertEquals(in, out);
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  public void testArrayReader(boolean genTypes) throws IOException {
    var in = new ArrayReader(new String[] { "Hello", "World!" }, new int[] { 1, 2, 3 });
    byte[] encoded = write(genTypes, in);
    ArrayReader out = read(genTypes, encoded);
    assertEquals(in, out);
  }

  public record BooleanTypes(boolean field1, Boolean field2) {
  }

  public record DoubleTypes(double field1, Double field2) {
  }

  public record CharacterTypes(char field1, Character field2) {
  }

  public record ByteTypes(byte field1, Byte field2) {
  }

  public record ShortTypes(short field1, Short field2) {
  }

  public record LongTypes(long field1, Long field2) {
  }

  public record IntegerTypes(int field1, Integer field2) {
  }

  public record FloatTypes(float field1, Float field2) {
  }

  public record ArrayReader(String[] field1, int[] field2) {
    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (!(o instanceof ArrayReader that))
        return false;
      return Objects.deepEquals(field2, that.field2) && Objects.deepEquals(field1, that.field1);
    }

    @Override
    public int hashCode() {
      return Objects.hash(Arrays.hashCode(field1), Arrays.hashCode(field2));
    }
  }

  public record ObjectType(String field1, byte[] field2) {
    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (!(o instanceof ObjectType that))
        return false;
      return Objects.equals(field1, that.field1) && Objects.deepEquals(field2, that.field2);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1, Arrays.hashCode(field2));
    }
  }

}
