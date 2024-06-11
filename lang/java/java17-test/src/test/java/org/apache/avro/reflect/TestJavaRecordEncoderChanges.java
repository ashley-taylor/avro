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

import org.apache.avro.AvroMissingFieldException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.stream.Stream;

import static org.junit.Assert.*;

import static org.apache.avro.reflect.RecordReadWriteUtil.read;
import static org.apache.avro.reflect.RecordReadWriteUtil.write;

public class TestJavaRecordEncoderChanges {

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  public void testBase(boolean genTypes) throws IOException {
    Base in = new Base("hello world", 42);
    byte[] encoded = write(genTypes, in, Base.class);
    Base decoded = read(genTypes, encoded);

    assertNotNull(decoded);
    assertEquals("hello world", decoded.field());
    assertEquals(42, decoded.field2());

  }

  private static Stream<Arguments> testCompatibleTypeChangeParameters() {
    return Stream.of(Arguments.of(false, BaseTypeChangeCompatible.class),
        Arguments.of(true, BaseTypeChangeCompatible.class), Arguments.of(false, BaseTypeChangeCompatibleClass.class));
  }

  @ParameterizedTest
  @MethodSource("testCompatibleTypeChangeParameters")
  public void testCompatibleTypeChange(boolean genTypes, Class<?> type) throws IOException {
    Base in = new Base("hello world", 42);
    byte[] encoded = write(genTypes, in, type);
    Compatible decoded = read(genTypes, encoded);

    assertNotNull(decoded);
    assertEquals("hello world", decoded.field());
    assertEquals(42, decoded.field2(), 0);

  }

  private static Stream<Arguments> testCastableTypeChangeParameters() {
    return Stream.of(Arguments.of(false, BaseTypeChangeCastable.class),
        Arguments.of(true, BaseTypeChangeCastable.class), Arguments.of(false, BaseTypeChangeCastableClass.class));
  }

  @ParameterizedTest
  @MethodSource("testCastableTypeChangeParameters")
  public void testCastableTypeChange(boolean genTypes, Class<?> type) {
    Base in = new Base("hello world", 42);
    byte[] encoded = write(genTypes, in, type);
    assertThrows(IllegalArgumentException.class, () -> read(genTypes, encoded));
  }

  private static Stream<Arguments> testWithDefaultParameters() {
    return Stream.of(Arguments.of(false, BaseWithDefault.class), Arguments.of(true, BaseWithDefault.class),
        Arguments.of(false, BaseWithDefaultClass.class));
  }

  @ParameterizedTest
  @MethodSource("testWithDefaultParameters")
  public void testWithDefault(boolean genTypes, Class<?> type) throws IOException {
    Base in = new Base("hello world", 42);
    byte[] encoded = write(genTypes, in, type);
    Default decoded = read(genTypes, encoded);

    assertNotNull(decoded);
    assertEquals("hello world", decoded.field());
    assertEquals(42, decoded.field2());
    assertEquals(0, decoded.field3(), 0); // avro default annotation seems to get ignored, matching class behaviour
    assertNull(decoded.field4()); // avro default annotation seems to get ignored, matching class behaviour

  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  public void testWithRemovedField(boolean genTypes) {
    Base in = new Base("hello world", 42);
    byte[] encoded = write(genTypes, in, BaseWithFieldRemoved.class);
    assertThrows(AvroMissingFieldException.class, () -> read(genTypes, encoded));
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  public void testWithRemovedFieldClass(boolean genTypes) {
    Base in = new Base("hello world", 42);
    byte[] encoded = write(genTypes, in, BaseWithFieldRemovedClass.class);
    assertThrows(NullPointerException.class, () -> read(genTypes, encoded)); // exceptions don't match
  }

  public record Base(String field, long field2) {
  }

  public record BaseTypeChangeCastable(String field, int field2) {
  }

  interface Default {
    String field();

    long field2();

    double field3();

    String field4();

  }

  public record BaseWithDefault(String field, long field2, @AvroDefault("7.6") double field3,
      @AvroDefault("\"Not a primitive\"") String field4) implements Default {
  }

  public static class BaseWithDefaultClass implements Default {
    String field;
    long field2;
    @AvroDefault("7.6")
    double field3;
    @AvroDefault("\"Not a primitive\"")
    String field4;

    @Override
    public String field() {
      return field;
    }

    @Override
    public long field2() {
      return field2;
    }

    @Override
    public double field3() {
      return field3;
    }

    @Override
    public String field4() {
      return field4;
    }
  }

  public record BaseWithFieldRemoved(String field) {
  }

  interface Compatible {
    String field();

    double field2();
  }

  public record BaseTypeChangeCompatible(String field, double field2) implements Compatible {
  }

  // to compare to class behaviour
  public static class BaseTypeChangeCompatibleClass implements Compatible {
    String field;
    double field2;

    @Override
    public String field() {
      return field;
    }

    @Override
    public double field2() {
      return field2;
    }
  }

  public static class BaseTypeChangeCastableClass {
    String field;
    int field2;
  }

  public static class BaseWithFieldRemovedClass {
    String field;
  }

}
