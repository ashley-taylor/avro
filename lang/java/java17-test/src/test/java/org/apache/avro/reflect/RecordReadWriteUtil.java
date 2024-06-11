package org.apache.avro.reflect;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

public class RecordReadWriteUtil {

  static <T> T read(boolean genTypes, byte[] toDecode) throws IOException {
    System.setProperty(ReflectRecordEncoding.GENERATE_BINDING, Boolean.toString(genTypes));
    var reflectData = new ReflectData();
    DatumReader<T> datumReader = new ReflectDatumReader<>(reflectData);
    try (DataFileStream<T> dataFileReader = new DataFileStream<>(new ByteArrayInputStream(toDecode, 0, toDecode.length),
        datumReader);) {
      dataFileReader.hasNext();
      return dataFileReader.next();
    } finally {
      System.setProperty(ReflectRecordEncoding.GENERATE_BINDING, "false");
    }
  }

  static <T> byte[] write(boolean genTypes, T custom) {
    return write(genTypes, custom.getClass(), custom);
  }

  static <T> byte[] write(boolean genTypes, Class<?> type, T custom) {
    System.setProperty(ReflectRecordEncoding.GENERATE_BINDING, Boolean.toString(genTypes));
    var reflectData = new ReflectData();

    Schema schema = reflectData.getSchema(type);

    ReflectDatumWriter<T> datumWriter = new ReflectDatumWriter<>(reflectData);
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataFileWriter<T> writer = new DataFileWriter<>(datumWriter)) {
      writer.create(schema, baos);
      writer.append(custom);
      writer.flush();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      System.setProperty(ReflectRecordEncoding.GENERATE_BINDING, "false");
    }
  }

  static <T> byte[] write(boolean genTypes, T custom, Class<?> asName) {

    System.setProperty(ReflectRecordEncoding.GENERATE_BINDING, Boolean.toString(genTypes));
    var reflectData = new ReflectData();
    var schema = reflectData.getSchema(custom.getClass());

    var schemaAs = reflectData.getSchema(asName);

    var fields = schema.getFields().stream()
        .map(field -> new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal(), field.order()))
        .toList();

    schemaAs = Schema.createRecord(schemaAs.getName(), schemaAs.getDoc(), schemaAs.getNamespace(), schemaAs.isError(),
        fields);

    ReflectDatumWriter<T> datumWriter = new ReflectDatumWriter<>(reflectData);

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataFileWriter<T> writer = new DataFileWriter<>(datumWriter)) {
      writer.create(schemaAs, baos);
      datumWriter.setSchema(schema);
      writer.append(custom);
      writer.flush();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      System.setProperty(ReflectRecordEncoding.GENERATE_BINDING, "false");
    }
  }
}
