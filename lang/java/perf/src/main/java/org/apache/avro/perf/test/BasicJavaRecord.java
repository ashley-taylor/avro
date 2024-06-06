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

public record BasicJavaRecord(double f1,
 double f2,
 double f3,
 int f4,
 int f5,
 int f6) {



  public BasicJavaRecord(final Random r) {

    this(r.nextDouble(),
     r.nextDouble(),
     r.nextDouble(),
     r.nextInt(),
     r.nextInt(),
     r.nextInt());
  }
}
