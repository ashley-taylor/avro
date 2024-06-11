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

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.avro.reflect.RecordReadWriteUtil.read;
import static org.apache.avro.reflect.RecordReadWriteUtil.write;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPolymorphicEncoding {

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  public void testPolymorphicEncoding(boolean genTypes) throws IOException {
    var cat = new Cat("Green");
    var dog = new Dog(5);
    {
      byte[] encoded = write(genTypes, Animal.class, cat);
      Animal decoded = read(genTypes, encoded);
      assertEquals(cat, decoded);
    }
    {
      byte[] encoded = write(genTypes, Animal.class, dog);
      Animal decoded = read(genTypes, encoded);
      assertEquals(dog, decoded);
    }
  }

  public sealed interface Animal permits Cat,Dog {
  }

  public record Dog(int size) implements Animal {
  }

  public record Cat(String color) implements Animal {
  }

}
