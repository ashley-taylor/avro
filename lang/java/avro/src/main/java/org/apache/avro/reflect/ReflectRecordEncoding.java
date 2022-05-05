package org.apache.avro.reflect;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.AvroMissingFieldException;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.ResolvingDecoder;

public class ReflectRecordEncoding extends CustomEncoding {

  private final List<FieldWriter> writer;
  private final Constructor<?> constructor;
  private List<FieldReader> reader;

  public ReflectRecordEncoding(Schema schema, Class<?> type) {
    this.schema = schema;

    this.writer = schema.getFields().stream().map(field -> {
      try {
        Method method = type.getMethod(field.name());
        return new FieldWriter(method, field.schema());
      } catch (ReflectiveOperationException e) {
        throw new AvroMissingFieldException("Field does not exist", field);
      }

    }).collect(Collectors.toList());

    // order of this matches default constructor find order mapping

    Field[] fields = type.getDeclaredFields();

    List<Class<?>> parameterTypes = new ArrayList<>(fields.length);

    Map<String, Integer> offsets = new HashMap<>();

    // need to know offset for mapping
    for (Field field : fields) {
      if (Modifier.isStatic(field.getModifiers())) {
        continue;
      }
      offsets.put(field.getName(), parameterTypes.size());
      parameterTypes.add(field.getType());
    }

    try {
      this.constructor = type.getDeclaredConstructor(parameterTypes.toArray(Class[]::new));

    } catch (NoSuchMethodException e) {
      throw new AvroRuntimeException(e);
    }

    this.reader = schema.getFields().stream().map(field -> {
      int offset = offsets.get(field.name());
      return new FieldReader(offset, field.schema());

    }).collect(Collectors.toList());

  }

  @Override
  protected void write(Object datum, Encoder out) throws IOException {
    throw new UnsupportedOperationException("No writer specified");
  }

  @Override
  protected void write(Object datum, Encoder out, ReflectDatumWriter writer) throws IOException {
    for (FieldWriter field : this.writer) {
      field.write(datum, out, writer);
    }
  }

  @Override
  protected Object read(Object reuse, Decoder in) throws IOException {
    throw new UnsupportedOperationException("No writer specified");
  }

  @Override
  protected Object read(Object reuse, ResolvingDecoder in, ReflectDatumReader reader) throws IOException {

    Object[] args = new Object[this.reader.size()];

    for (FieldReader field : this.reader) {
      field.read(in, reader, args);
    }

    try {
      return this.constructor.newInstance(args);
    } catch (ReflectiveOperationException e) {
      throw new AvroRuntimeException(e);
    }
  }

  private static class FieldWriter {
    private final Method method;
    private final Schema schema;

    public FieldWriter(Method method, Schema schema) {
      this.method = method;
      this.schema = schema;
    }

    void write(Object datum, Encoder out, ReflectDatumWriter writer) throws IOException {
      try {
        Object obj = method.invoke(datum);
        writer.write(schema, obj, out);
      } catch (ReflectiveOperationException e) {
        throw new AvroRuntimeException("Could not invoke", e);
      }

    }
  }

  private static class FieldReader {
    private final int constructorOffset;
    private final Schema schema;

    public FieldReader(int constructorOffset, Schema schema) {
      this.constructorOffset = constructorOffset;
      this.schema = schema;
    }

    void read(ResolvingDecoder in, ReflectDatumReader reader, Object[] constructorArgs) throws IOException {
      Object obj = reader.read(null, schema, in);
      constructorArgs[constructorOffset] = obj;
    }
  }

}
