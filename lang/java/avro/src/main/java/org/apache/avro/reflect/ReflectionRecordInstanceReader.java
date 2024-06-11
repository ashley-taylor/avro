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
import java.lang.reflect.Constructor;
import java.util.List;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.io.ResolvingDecoder;

public class ReflectionRecordInstanceReader implements RecordInstanceReader {

  private final List<RecordFieldBuilder.FieldInfo> reader;
  private final Constructor<?> constructor;

  ReflectionRecordInstanceReader(List<RecordFieldBuilder.FieldInfo> fields, Constructor<?> constructor) {
    this.reader = fields;
    this.constructor = constructor;
  }

  @Override
  public Object read(ResolvingDecoder in, ReflectDatumReader<?> reader) throws IOException {
    Object[] args = new Object[this.reader.size()];

    for (var field : this.reader) {
      args[field.getConstructorOffset()] = field.getReader().read(in, reader);
    }

    try {
      return this.constructor.newInstance(args);
    } catch (ReflectiveOperationException e) {
      throw new AvroRuntimeException(e);
    }
  }
}
