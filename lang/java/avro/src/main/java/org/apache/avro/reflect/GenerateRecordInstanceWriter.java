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

import static org.objectweb.asm.Type.getDescriptor;
import static org.objectweb.asm.Type.getInternalName;
import static org.objectweb.asm.Type.getMethodDescriptor;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.avro.reflect.RecordFieldBuilder.FieldInfo;
import org.apache.avro.reflect.RecordFieldBuilder.LookupKey;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

public class GenerateRecordInstanceWriter implements Opcodes {

  private static final Map<LookupKey, Caller> SIMPLE_WRITER = new HashMap<>();
  private static final int THIS = 0;
  private static final int LIST = 1;
  private static final int DATUM = 1;
  private static final int ENCODER = 2;
  private static final int WRITER = 3;

  static {
    SIMPLE_WRITER.put(new LookupKey(int.class, Schema.Type.INT), new Caller("writeInt", int.class));
    SIMPLE_WRITER.put(new LookupKey(float.class, Schema.Type.FLOAT), new Caller("writeFloat", float.class));
    SIMPLE_WRITER.put(new LookupKey(boolean.class, Schema.Type.BOOLEAN), new Caller("writeBoolean", boolean.class));
    SIMPLE_WRITER.put(new LookupKey(double.class, Schema.Type.DOUBLE), new Caller("writeDouble", double.class));
    SIMPLE_WRITER.put(new LookupKey(long.class, Schema.Type.LONG), new Caller("writeLong", long.class));
    SIMPLE_WRITER.put(new LookupKey(String.class, Schema.Type.STRING), new Caller("writeString", String.class));
  }

  RecordInstanceWriter generate(List<FieldInfo> fields, Class<?> type) throws ReflectiveOperationException {
    // store as a static class within the class to remove visibility issues
    final String fullClassName = type.getName() + "$GeneratedAvroWriter";
    var bytes = generateClass(fullClassName, fields, type);

    var loader = new ClassLoader(type.getClassLoader()) {
      @Override
      public Class<?> findClass(String name) {
        return defineClass(name, bytes, 0, bytes.length);
      }
    };
    var clazz = loader.loadClass(fullClassName);
    var generatedConstructor = clazz.getConstructor(List.class);

    return (RecordInstanceWriter) generatedConstructor.newInstance(fields);
  }

  static byte[] generateClass(String fullClassName, List<FieldInfo> fields, Class<?> type)
      throws ReflectiveOperationException {
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);

    String className = fullClassName.replace('.', '/');

    cw.visit(V17, ACC_PUBLIC + ACC_SUPER + ACC_STATIC, className, null, getInternalName(Object.class),
        new String[] { getInternalName(RecordInstanceWriter.class) });

    cw.visitSource(null, null);

    generateFields(fields, cw);

    generateConstructor(fields, cw, className);

    generateWriter(fields, type, cw, className);

    cw.visitEnd();

    return cw.toByteArray();
  }

  private static void generateFields(List<FieldInfo> fields, ClassWriter cw) {
    for (var field : fields) {
      var lookup = new LookupKey(field);

      if (!SIMPLE_WRITER.containsKey(lookup)) {
        cw.visitField(ACC_PRIVATE + ACC_FINAL, field.getName(), getDescriptor(RecordInstanceWriter.class), null, null);
      }
    }
  }

  private static void generateConstructor(List<FieldInfo> fields, ClassWriter cw, String className)
      throws NoSuchMethodException {
    var mv = cw.visitMethod(ACC_PUBLIC, "<init>", "(Ljava/util/List;)V",
        "(Ljava/util/List<Lorg/apache/avro/reflect/RecordInstanceWriter;>;)V", null);
    mv.visitVarInsn(ALOAD, THIS);
    mv.visitMethodInsn(INVOKESPECIAL, getInternalName(Object.class), "<init>", "()V", false);

    for (int i = 0; i < fields.size(); i++) {
      var field = fields.get(i);
      var lookup = new LookupKey(field);

      if (SIMPLE_WRITER.containsKey(lookup)) {
        continue;
      }
      mv.visitVarInsn(ALOAD, THIS);
      mv.visitVarInsn(ALOAD, LIST);
      mv.visitIntInsn(SIPUSH, i);
      mv.visitMethodInsn(INVOKEINTERFACE, getInternalName(List.class), "get",
          getMethodDescriptor(List.class.getMethod("get", int.class)), true);
      mv.visitTypeInsn(CHECKCAST, getInternalName(FieldInfo.class));
      mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(FieldInfo.class), "getWriter",
          getMethodDescriptor(FieldInfo.class.getMethod("getWriter")), false);
      mv.visitFieldInsn(PUTFIELD, className, field.getName(), getDescriptor(RecordInstanceWriter.class));
    }
    mv.visitInsn(RETURN);
    mv.visitMaxs(0, 0);
    mv.visitEnd();
  }

  private static void generateWriter(List<FieldInfo> fields, Class<?> type, ClassWriter cw, String className)
      throws NoSuchMethodException {
    var description = getMethodDescriptor(
        RecordInstanceWriter.class.getMethod("write", Object.class, Encoder.class, ReflectDatumWriter.class));
    var mv = cw.visitMethod(ACC_PUBLIC, "write", description, null,
        new String[] { getInternalName(IOException.class) });
    mv.visitCode();

    for (var field : fields) {
      var method = type.getMethod(field.getName());
      var lookup = new LookupKey(field);
      var caller = SIMPLE_WRITER.get(lookup);
      if (caller != null) {
        mv.visitVarInsn(ALOAD, ENCODER);
        mv.visitVarInsn(ALOAD, DATUM);
        mv.visitTypeInsn(CHECKCAST, getInternalName(type));
        mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(type), method.getName(), getMethodDescriptor(method), false);
        mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(Encoder.class), caller.getMethod(), caller.getDescriptor(),
            false);
      } else {
        mv.visitVarInsn(ALOAD, THIS);
        mv.visitFieldInsn(GETFIELD, className, field.getName(), getDescriptor(RecordInstanceWriter.class));
        mv.visitVarInsn(ALOAD, DATUM);
        mv.visitTypeInsn(CHECKCAST, getInternalName(type));
        mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(type), method.getName(), getMethodDescriptor(method), false);

        if (field.getType().isPrimitive()) {
          castPrimitive(field, mv);
        }

        mv.visitVarInsn(ALOAD, ENCODER);
        mv.visitVarInsn(ALOAD, WRITER);
        mv.visitMethodInsn(INVOKEINTERFACE, getInternalName(RecordInstanceWriter.class), "write", description, true);
      }
    }
    mv.visitInsn(RETURN);
    mv.visitMaxs(0, 0);
    mv.visitEnd();
  }

  private static void castPrimitive(FieldInfo field, MethodVisitor mv) throws NoSuchMethodException {
    var fType = field.getType();
    if (fType.equals(double.class)) {
      addPrimitiveCast(mv, Double.class, double.class);
    }
    if (fType.equals(float.class)) {
      addPrimitiveCast(mv, Float.class, float.class);
    }
    if (fType.equals(long.class)) {
      addPrimitiveCast(mv, Long.class, long.class);
    }
    if (fType.equals(boolean.class)) {
      addPrimitiveCast(mv, Boolean.class, boolean.class);
    }
    if (fType.equals(int.class)) {
      addPrimitiveCast(mv, Integer.class, int.class);
    }
    if (fType.equals(byte.class)) {
      addPrimitiveCast(mv, Byte.class, byte.class);
    }
    if (fType.equals(short.class)) {
      addPrimitiveCast(mv, Short.class, short.class);
    }
    if (fType.equals(char.class)) {
      addPrimitiveCast(mv, Character.class, char.class);
    }
  }

  private static void addPrimitiveCast(MethodVisitor mv, Class<?> type, Class<?> primitiveType)
      throws NoSuchMethodException {
    mv.visitMethodInsn(INVOKESTATIC, getInternalName(type), "valueOf",
        getMethodDescriptor(type.getMethod("valueOf", primitiveType)), false);
  }

  private static class Caller {

    private final String method;
    private final String descriptor;

    public Caller(String method, Class<?>... type) {
      this.method = method;
      try {
        this.descriptor = getMethodDescriptor(Encoder.class.getMethod(method, type));
      } catch (NoSuchMethodException | SecurityException e) {

        throw new RuntimeException(e);
      }
    }

    public String getMethod() {
      return method;
    }

    public String getDescriptor() {
      return descriptor;
    }
  }
}
