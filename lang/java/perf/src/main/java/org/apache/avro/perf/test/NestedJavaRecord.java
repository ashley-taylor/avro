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

package org.apache.avro.perf.test;

import java.util.Random;

public record NestedJavaRecord(double f1, double f2, Child[] children) {

  public NestedJavaRecord(final Random r) {

    this(r.nextDouble(), r.nextDouble(),
        new Child[] { new Child1(r.nextInt(), r.nextFloat()), new Child2(r.nextBoolean(), r.nextLong()) });
  }

  sealed interface Child permits Child1,Child2 {
  }

  record Child1(int f1, float f2) implements Child {

  }

  record Child2(boolean f1, long f2) implements Child {

  }
}
