package org.apache.avro.reflect;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.avro.AvroMissingFieldException;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.ClassUtils;

class RecordFieldBuilder {

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

  private static final Map<LookupKey, RecordInstanceReader> SIMPLE_READER = new HashMap<>();

  static {
    SIMPLE_READER.put(new LookupKey(Boolean.class, Type.BOOLEAN), (in, reader) -> in.readBoolean());
    SIMPLE_READER.put(new LookupKey(boolean.class, Type.BOOLEAN), (in, reader) -> in.readBoolean());
    SIMPLE_READER.put(new LookupKey(double.class, Type.DOUBLE), (in, reader) -> in.readDouble());
    SIMPLE_READER.put(new LookupKey(Double.class, Type.DOUBLE), (in, reader) -> in.readDouble());
    SIMPLE_READER.put(new LookupKey(long.class, Type.LONG), (in, reader) -> in.readLong());
    SIMPLE_READER.put(new LookupKey(Long.class, Type.LONG), (in, reader) -> in.readLong());
    SIMPLE_READER.put(new LookupKey(int.class, Type.INT), (in, reader) -> in.readInt());
    SIMPLE_READER.put(new LookupKey(Integer.class, Type.INT), (in, reader) -> in.readInt());
    SIMPLE_READER.put(new LookupKey(float.class, Type.FLOAT), (in, reader) -> in.readFloat());
    SIMPLE_READER.put(new LookupKey(Float.class, Type.FLOAT), (in, reader) -> in.readFloat());
    SIMPLE_READER.put(new LookupKey(String.class, Type.STRING), (in, reader) -> in.readString());
    SIMPLE_READER.put(new LookupKey(byte[].class, Type.BYTES), (in, reader) -> in.readBytes(null).array());
  }

  static Constructor<?> getRecordConstructor(Class<?> type) throws ReflectiveOperationException {
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

    return type.getConstructor(parameterTypes.toArray(new Class[0]));
  }

  static List<FieldInfo> buildFieldInfo(Class<?> type, Schema schema) {
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

    return schema.getFields().stream().map(field -> {
      int offset = offsets.get(field.name());

      try {
        Field classField = type.getDeclaredField(field.name());

        return new FieldInfo(field, classField, buildReader(field.schema(), classField), buildWriter(classField, field),
            offset);
      } catch (ReflectiveOperationException | IOException e) {
        throw new AvroRuntimeException("Could not instantiate custom Encoding");
      }
    }).collect(Collectors.toList());
  }

  private static RecordInstanceWriter buildWriter(Field classField, Schema.Field field) {
    try {
      classField.setAccessible(true);
      AvroEncode enc = classField.getAnnotation(AvroEncode.class);
      if (enc != null)
        return new CustomEncodedFieldWriter(enc.using().getDeclaredConstructor().newInstance());
      return new ReflectFieldWriter(field.schema());
    } catch (ReflectiveOperationException e) {
      throw new AvroMissingFieldException("Field does not exist", field);
    }
  }

  private static RecordInstanceReader buildReader(org.apache.avro.Schema schema, Field classField)
      throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException,
      IOException {
    AvroEncode enc = classField.getAnnotation(AvroEncode.class);
    if (enc != null)
      return new CustomEncodedFieldReader(enc.using().getDeclaredConstructor().newInstance());

    var simple = SIMPLE_READER.get(new LookupKey(classField.getType(), schema.getType()));

    if (simple != null) {
      return simple;
    }

    if (schema.getType() == Schema.Type.UNION) {
      return new FastUnionReader(schema, classField);
    }

    if (schema.getType() == Schema.Type.ARRAY && classField.getType().isArray()) {
      return new FastArrayReader(schema, classField);
    }

    if (schema.getType() == Schema.Type.RECORD) {
      try {
        Class<?> c = ClassUtils.forName(classField.getDeclaringClass().getClassLoader(),
            SpecificData.getClassName(schema));
        if (IS_RECORD_METHOD != null && IS_RECORD_METHOD.invoke(c).equals(true)) {
          return new CustomEncodedFieldReader(new ReflectRecordEncoding(c, schema));
        }
      } catch (ClassNotFoundException e) {
        // Avro will throw this same error later
      }
    }

    return new ReflectFieldReader(schema);
  }

  private static class ReflectFieldReader implements RecordInstanceReader {

    private final Schema schema;

    public ReflectFieldReader(Schema schema) {
      this.schema = schema;
    }

    @Override
    public Object read(ResolvingDecoder in, ReflectDatumReader<?> reader) throws IOException {
      return reader.read(null, schema, in);
    }
  }

  private static class FastUnionReader implements RecordInstanceReader {

    private final RecordInstanceReader[] unions;

    public FastUnionReader(Schema schema, Field classField) throws InstantiationException, IllegalAccessException,
        InvocationTargetException, NoSuchMethodException, IOException {
      var types = schema.getTypes();

      this.unions = new RecordInstanceReader[types.size()];

      for (int i = 0; i < types.size(); i++) {
        this.unions[i] = buildReader(types.get(i), classField);
      }
    }

    @Override
    public Object read(ResolvingDecoder in, ReflectDatumReader<?> reader) throws IOException {
      return unions[in.readIndex()].read(in, reader);
    }
  }

  private static class FastArrayReader implements RecordInstanceReader {

    private final RecordInstanceReader inner;
    private Field classField;

    public FastArrayReader(Schema schema, Field classField) throws InstantiationException, IllegalAccessException,
        InvocationTargetException, NoSuchMethodException, IOException {
      this.classField = classField;
      var type = schema.getElementType();
      this.inner = buildReader(type, classField);
    }

    @Override
    public Object read(ResolvingDecoder in, ReflectDatumReader<?> reader) throws IOException {
      Object[] array = null;
      for (long i = in.readArrayStart(); i != 0; i = in.arrayNext()) {
        int j = 0;
        if (array == null) {
          array = (Object[]) Array.newInstance(classField.getType().getComponentType(), (int) i);
        } else {
          j = array.length;
          array = Arrays.copyOf(array, (int) i + array.length, (Class<Object[]>) classField.getType());
        }
        for (; j < i; j++) {
          array[j] = inner.read(in, reader);
        }
      }
      return array;
    }
  }

  private static class CustomEncodedFieldReader implements RecordInstanceReader {

    private final CustomEncoding<?> encoding;

    public CustomEncodedFieldReader(CustomEncoding<?> encoding) {
      this.encoding = encoding;
    }

    @Override
    public Object read(ResolvingDecoder in, ReflectDatumReader<?> reader) throws IOException {
      return encoding.read(null, in, reader);
    }
  }

  private static class ReflectFieldWriter implements RecordInstanceWriter {

    private final Schema schema;

    public ReflectFieldWriter(Schema schema) {
      this.schema = schema;
    }

    @Override
    public void write(Object datum, Encoder out, ReflectDatumWriter<?> writer) throws IOException {
      writer.write(schema, datum, out);
    }
  }

  private static class CustomEncodedFieldWriter implements RecordInstanceWriter {

    private final CustomEncoding<?> encoding;

    public CustomEncodedFieldWriter(CustomEncoding<?> encoding) {
      this.encoding = encoding;
    }

    @Override
    public void write(Object datum, Encoder out, ReflectDatumWriter<?> writer) throws IOException {
      encoding.write(datum, out);
    }
  }

  private static class LookupKey {

    private final Class<?> klass;
    private final Schema.Type type;

    public LookupKey(Class<?> klass, Type type) {
      super();
      this.klass = klass;
      this.type = type;
    }

    @Override
    public int hashCode() {
      return Objects.hash(klass, type);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      LookupKey other = (LookupKey) obj;
      return Objects.equals(klass, other.klass) && type == other.type;
    }
  }

  protected static class FieldInfo {

    private final Schema.Field field;
    private final Field recordField;
    private final RecordInstanceReader reader;
    private final RecordInstanceWriter writer;
    private final int schemaOffset;
    private final int constructorOffset;

    public FieldInfo(Schema.Field field, Field recordField, RecordInstanceReader reader, RecordInstanceWriter writer,
        int constructorOffset) {
      this.field = field;
      this.recordField = recordField;
      this.reader = reader;
      this.writer = writer;
      this.schemaOffset = field.pos();
      this.constructorOffset = constructorOffset;
    }

    public Schema.Field getField() {
      return field;
    }

    public Schema getSchema() {
      return field.schema();
    }

    public Field getRecordField() {
      return recordField;
    }

    public Class<?> getType() {
      return recordField.getType();
    }

    public int getConstructorOffset() {
      return constructorOffset;
    }

    public int getSchemaOffset() {
      return schemaOffset;
    }

    public RecordInstanceReader getReader() {
      return reader;
    }

    public RecordInstanceWriter getWriter() {
      return writer;
    }
  }
}
