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

import org.apache.avro.AvroRecordEncoderGenerator;
import org.apache.avro.AvroRecordEncoderProvider;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;

import java.lang.reflect.Method;
import java.util.Optional;

public class ReflectJavaRecordEncoderProvider implements AvroRecordEncoderProvider {
  @Override
  public AvroRecordEncoderGenerator create(SpecificData specificData) {
    return new JavaRecordEncoderGenerator();
  }

  @Override
  public int priority() {
    return 10;
  }

  private static class JavaRecordEncoderGenerator implements AvroRecordEncoderGenerator {

    private static final Method IS_RECORD_METHOD;

    static {
      Class<? extends Class> classClass = SpecificData.class.getClass();
      Method isRecord;
      try {
        isRecord = classClass.getMethod("isRecord");
      } catch (NoSuchMethodException e) {
        isRecord = null;
      }
      IS_RECORD_METHOD = isRecord;

    }

    @Override
    public Optional<CustomEncoding<?>> get(Class<?> c, Optional<Schema> schema) {
      try {
        if (IS_RECORD_METHOD != null && IS_RECORD_METHOD.invoke(c).equals(true)) {
          CustomEncoding<Object> encoder = new ReflectRecordEncoding(c);
          if (schema.isPresent()) {
            encoder = encoder.setSchema(schema.get());
          }
          return Optional.of(encoder);
        }
      } catch (ReflectiveOperationException e) {
        throw new AvroRuntimeException(e);
      }

      return Optional.empty();
    }
  }
}
