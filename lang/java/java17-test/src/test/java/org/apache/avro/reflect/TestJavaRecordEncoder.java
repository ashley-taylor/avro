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

import org.apache.avro.file.DataFileWriter.AppendWriteException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.avro.reflect.RecordReadWriteUtil.read;
import static org.apache.avro.reflect.RecordReadWriteUtil.write;
import static org.junit.jupiter.api.Assertions.*;

public class TestJavaRecordEncoder {

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  public void testRecord(boolean genTypes) throws IOException {
    Custom in = new Custom("hello world");
    byte[] encoded = write(genTypes, in);
    Custom decoded = read(genTypes, encoded);

    assertNotNull(decoded);
    assertEquals("hello world", decoded.field());
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  public void testRecordWithNulls(boolean genTypes) throws IOException {
    var in = new CustomWithNull("hello world", null);
    byte[] encoded = write(genTypes, in);
    CustomWithNull decoded = read(genTypes, encoded);

    assertNotNull(decoded);
    assertEquals("hello world", decoded.field());
    assertNull(decoded.field2());
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  public void testNonNullErrors(boolean genTypes) throws IOException {
    var in = new CustomWithNull(null, "pass");
    assertThrows(AppendWriteException.class, () -> {
      write(genTypes, in);
    });
  }

  public record Custom(String field) {
  }

  public record CustomWithNull(String field, @Nullable String field2) {
  }

}
