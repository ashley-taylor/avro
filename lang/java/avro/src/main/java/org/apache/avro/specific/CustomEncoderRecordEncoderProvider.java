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
package org.apache.avro.specific;

import org.apache.avro.AvroRecordEncoderGenerator;
import org.apache.avro.AvroRecordEncoderProvider;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.reflect.AvroEncode;
import org.apache.avro.reflect.CustomEncoding;

import java.util.Optional;

public class CustomEncoderRecordEncoderProvider implements AvroRecordEncoderProvider {
  @Override
  public AvroRecordEncoderGenerator create(SpecificData specificData) {
    return new CustomEncoderRecordProvider();
  }

  @Override
  public int priority() {
    return 0;
  }

  private static class CustomEncoderRecordProvider implements AvroRecordEncoderGenerator {

    private AvroEncode getAvroEncode(Class<?> c) {
      while (c != null && !c.equals(Object.class)) {
        AvroEncode avroEncode = c.getAnnotation(AvroEncode.class);
        if (avroEncode != null) {
          return avroEncode;
        }
        if (c.getSuperclass() != null) {
          avroEncode = getAvroEncode(c.getSuperclass());
        }
        if (avroEncode != null) {
          return avroEncode;
        }
        for (Class<?> inter : c.getInterfaces()) {
          avroEncode = getAvroEncode(inter);
          if (avroEncode != null) {
            return avroEncode;
          }
        }
        c = c.getSuperclass();
      }

      return null;

    }

    private CustomEncoding<?> getCustomEncoding(Class<?> c) {
      try {
        AvroEncode avroEncode = getAvroEncode(c);
        if (avroEncode != null) {
          // first see if constructor that takes the class as an argument exists
          try {
            return avroEncode.using().getDeclaredConstructor(Class.class).newInstance(c);
          } catch (NoSuchMethodException e) {
            // zero argument constructor
            return avroEncode.using().getDeclaredConstructor().newInstance();
          }
        } else {
          return null;
        }
      } catch (ReflectiveOperationException e) {
        throw new AvroRuntimeException(e);
      }
    }

    @Override
    public Optional<CustomEncoding<?>> get(Class<?> c, Optional<Schema> schema) {
      var encoding = getCustomEncoding(c);
      if (encoding != null) {

        if (schema.isPresent()) {
          encoding = encoding.setSchema(schema.get());
          return Optional.of(encoding);
        }

      }
      return Optional.empty();
    }
  }
}
