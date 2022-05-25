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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.reflect.AvroSubTypes.Type;
import org.apache.avro.specific.SpecificData;

public class PolymorphicEncoding extends CustomEncoding<Object> {

  private static final String POLYMORPHIC = "polymorphic";
private static final Method IS_SEALED_METHOD;
  private static final Method GET_PERMITTED_SUBCLASSES_METHOD;

  static {
    Class<? extends Class> classClass = SpecificData.class.getClass();
    Method isSealed;
    Method getPermittedSubclasses;
    try {
      isSealed = classClass.getMethod("isSealed");
      getPermittedSubclasses = classClass.getMethod("getPermittedSubclasses");
    } catch (NoSuchMethodException e) {
      isSealed = null;
      getPermittedSubclasses = null;
    }
    IS_SEALED_METHOD = isSealed;
    GET_PERMITTED_SUBCLASSES_METHOD = getPermittedSubclasses;

  }

  private final Map<Class<?>, OrdinalSchema> schemas;
  private final List<Schema> unionSchema;

  public PolymorphicEncoding(Class<?> type) {
    // find annotation
    type = getClassWithEncodingAnnotation(type);

    if (type == null) {
      throw new AvroRuntimeException("Could not find call or interface with attached Polymorphic encoding.");
    }

    String name = type.getSimpleName();
    String space = type.getPackage() == null ? "" : type.getPackage().getName();
    if (type.getEnclosingClass() != null) // nested class
      space = type.getEnclosingClass().getName().replace('$', '.');

    AvroSubTypes subTypes = type.getAnnotation(AvroSubTypes.class);
    ReflectData reflectData = ReflectData.get();

    List<Class<?>> types = new ArrayList<>();
    if (subTypes == null) {
      // automatic sealed class polymorphic
      try {
        if (IS_SEALED_METHOD != null && Boolean.TRUE.equals(IS_SEALED_METHOD.invoke(type))) {
          Class<?>[] subClasses = (Class<?>[]) GET_PERMITTED_SUBCLASSES_METHOD.invoke(type);
          for (Class<?> subType : subClasses) {
            types.add(subType);
          }
        } else {
          throw new AvroRuntimeException("No @AvroSubTypes annotation found or sealed interface");
        }
      } catch (ReflectiveOperationException e) {
        throw new AvroRuntimeException(e);
      }
    } else {
      for (Type subType : subTypes.value()) {
        types.add(subType.value());
      }
    }

    Map<Class<?>, OrdinalSchema> ordinals = new HashMap<>();
    List<Schema> unionSchema = new ArrayList<>();
    for (Class<?> subType : types) {
      Schema subTypeSchema = reflectData.createSchemaViaReflection(subType, new HashMap<>());
      ordinals.put(subType, new OrdinalSchema(ordinals.size(), subTypeSchema));
      unionSchema.add(subTypeSchema);
    }

    Field typeField = new Field("type", Schema.createUnion(unionSchema));

    schema = Schema.createRecord(name, "", space, false, Arrays.asList(typeField));
    schema.addProp(POLYMORPHIC, Boolean.TRUE.toString());
    this.schemas = ordinals;
    this.unionSchema = unionSchema;

  }

  public PolymorphicEncoding(Map<Class<?>, OrdinalSchema> schemas, List<Schema> unionSchema) {
    this.schemas = schemas;
    this.unionSchema = unionSchema;

  }

  @Override
  public CustomEncoding<Object> setSchema(Schema schema) {
    if (Boolean.parseBoolean(schema.getProp(POLYMORPHIC))) {
      return new PolymorphicEncoding(this.schemas, schema.getField("type").schema().getTypes());
    } else {
      return null;
    }
  }

  @Override
  protected void write(Object datum, Encoder out) throws IOException {
    throw new UnsupportedOperationException("No writer specified");
  }

  @Override
  protected void write(Object datum, Encoder out, ReflectDatumWriter writer) throws IOException {
    OrdinalSchema ordinal = this.schemas.get(datum.getClass());
    if (ordinal == null) {
      throw new AvroRuntimeException(
          "PolymorphicEncoding is not configured for type " + datum.getClass().getSimpleName());
    }
    out.writeIndex(ordinal.ordinal);
    writer.write(ordinal.schema, datum, out);
  }

  @Override
  protected Object read(Object reuse, Decoder in) throws IOException {
    throw new UnsupportedOperationException("No reader specified");
  }

  @Override
  protected Object read(Object reuse, ResolvingDecoder in, ReflectDatumReader reader) throws IOException {
    int index = in.readIndex();

    return reader.read(null, unionSchema.get(index), in);
  }

  private static Class<?> getClassWithEncodingAnnotation(Class<?> c) {
    while (c != null && !c.equals(Object.class)) {
      AvroEncode encoding = c.getAnnotation(AvroEncode.class);
      if (encoding != null && encoding.using().equals(PolymorphicEncoding.class)) {
        return c;
      }
      if (c.getSuperclass() != null) {
        Class<?> type = getClassWithEncodingAnnotation(c.getSuperclass());
        if (type != null) {
          return type;
        }
      }
      for (Class<?> inter : c.getInterfaces()) {
        Class<?> type = getClassWithEncodingAnnotation(inter);
        if (type != null) {
          return type;
        }
      }
      c = c.getSuperclass();
    }

    return null;

  }

  private static final class OrdinalSchema {
    private final int ordinal;
    private final Schema schema;

    public OrdinalSchema(int ordinal, Schema schema) {
      this.ordinal = ordinal;
      this.schema = schema;
    }

  }

}
