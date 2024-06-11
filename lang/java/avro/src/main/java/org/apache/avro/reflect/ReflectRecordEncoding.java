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

import java.io.IOException;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.ResolvingDecoder;

public class ReflectRecordEncoding extends CustomEncoding<Object> {

  public static final String GENERATE_BINDING = "org.apache.avro.generatebinding";

  private final boolean generateBinding = "true".equalsIgnoreCase(System.getProperty(GENERATE_BINDING));

  private final Class<?> type;
  private final RecordInstanceReader reader;
  private final RecordInstanceWriter writer;

  public ReflectRecordEncoding(Class<?> type) {

    this.type = type;
    this.writer = null;
    this.reader = null;
  }

  public ReflectRecordEncoding(Class<?> type, Schema schema) {
    this.type = type;
    this.schema = schema;
    var fields = RecordFieldBuilder.buildFieldInfo(type, schema);
    try {
      if (generateBinding) {
        this.writer = new GenerateRecordInstanceWriter().generate(fields, type);
        this.reader = new GenerateRecordInstanceReader().generate(fields,
            RecordFieldBuilder.getRecordConstructor(type));

      } else {
        this.writer = new ReflectionRecordInstanceWriter(fields);
        this.reader = new ReflectionRecordInstanceReader(fields, RecordFieldBuilder.getRecordConstructor(type));
      }
    } catch (ReflectiveOperationException e) {
      throw new AvroRuntimeException(e);
    }

  }

  @Override
  public CustomEncoding<Object> setSchema(Schema schema) {
    return new ReflectRecordEncoding(type, schema);
  }

  @Override
  protected void write(Object datum, Encoder out) throws IOException {
    throw new UnsupportedOperationException("No writer specified");
  }

  @Override
  protected void write(Object datum, Encoder out, ReflectDatumWriter writer) throws IOException {
    this.writer.write(datum, out, writer);
  }

  @Override
  protected Object read(Object reuse, Decoder in) throws IOException {
    throw new UnsupportedOperationException("No reader specified");
  }

  @Override
  protected Object read(Object reuse, ResolvingDecoder in, ReflectDatumReader reader) throws IOException {
    return this.reader.read(in, reader);
  }

}
