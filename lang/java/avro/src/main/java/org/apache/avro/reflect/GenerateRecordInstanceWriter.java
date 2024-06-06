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

import org.apache.avro.io.Encoder;
import org.apache.avro.reflect.RecordFieldBuilder.FieldInfo;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;

public class GenerateRecordInstanceWriter implements Opcodes {

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

  private static final Map<Class<?>, Caller> SIMPLE_READER = new HashMap<>();

  static {
    SIMPLE_READER.put(int.class, new Caller("writeInt", int.class));
    SIMPLE_READER.put(float.class, new Caller("writeFloat", float.class));
    SIMPLE_READER.put(boolean.class, new Caller("writeBoolean", boolean.class));
    SIMPLE_READER.put(double.class, new Caller("writeDouble", double.class));
    SIMPLE_READER.put(long.class, new Caller("writeLong", long.class));
    SIMPLE_READER.put(String.class, new Caller("writeString", String.class));
  }

  public RecordInstanceWriter generate(List<RecordFieldBuilder.FieldInfo> fields, Class<?> type)
      throws ReflectiveOperationException {
    var packageName = "org.apache.avro.generated." + type.getPackageName();
    final String fullClassName = packageName + "." + type.getSimpleName() + "Writer";

    var bytes = dump(fullClassName, fields, type);

    var loader = new ClassLoader(type.getClassLoader()) {
      @Override
      public Class<?> findClass(String name) {
        return defineClass(name, bytes, 0, bytes.length);
      }
    };
    var clazz = loader.loadClass(fullClassName);
    var generatedConstructor = clazz.getConstructor(List.class);
    var instance = (RecordInstanceWriter) generatedConstructor.newInstance(fields);

    return instance;
  }

  public static byte[] dump(String fullClassName, List<FieldInfo> fields, Class<?> type)
      throws ReflectiveOperationException {
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);

    String className = fullClassName.replace('.', '/');

    cw.visit(V17, ACC_PUBLIC + ACC_SUPER, className, null, getInternalName(Object.class),
        new String[] { getInternalName(RecordInstanceWriter.class) });

    cw.visitSource(null, null);

    for (var field : fields) {
      if (!SIMPLE_READER.containsKey(field.getType())) {
        cw.visitField(ACC_PRIVATE + ACC_FINAL, field.getField().name(), getDescriptor(RecordInstanceWriter.class), null,
            null);
      }
    }

    {
      var mv = cw.visitMethod(ACC_PUBLIC, "<init>", "(Ljava/util/List;)V",
          "(Ljava/util/List<Lorg/apache/avro/reflect/RecordInstanceWriter;>;)V", null);
      mv.visitVarInsn(ALOAD, 0);
      mv.visitMethodInsn(INVOKESPECIAL, getInternalName(Object.class), "<init>", "()V", false);

      for (int i = 0; i < fields.size(); i++) {
        var field = fields.get(i);
        if (SIMPLE_READER.containsKey(field.getType())) {
          continue;
        }
        mv.visitVarInsn(ALOAD, 0);
        mv.visitVarInsn(ALOAD, 1);
        mv.visitIntInsn(SIPUSH, i);
        mv.visitMethodInsn(INVOKEINTERFACE, getInternalName(List.class), "get",
            getMethodDescriptor(List.class.getMethod("get", int.class)), true);
        mv.visitTypeInsn(CHECKCAST, getInternalName(RecordFieldBuilder.FieldInfo.class));
        mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(RecordFieldBuilder.FieldInfo.class), "getWriter",
            getMethodDescriptor(RecordFieldBuilder.FieldInfo.class.getMethod("getWriter")), false);
        mv.visitFieldInsn(PUTFIELD, className, field.getField().name(), getDescriptor(RecordInstanceWriter.class));
      }
      mv.visitInsn(RETURN);
      mv.visitMaxs(0, 0);
      mv.visitEnd();
    }

    {
      var description = getMethodDescriptor(
          RecordInstanceWriter.class.getMethod("write", Object.class, Encoder.class, ReflectDatumWriter.class));
      var mv = cw.visitMethod(ACC_PUBLIC, "write", description, null,
          new String[] { getInternalName(IOException.class) });
      mv.visitCode();

      for (var field : fields) {
        var method = type.getMethod(field.getField().name());

        var caller = SIMPLE_READER.get(field.getType());
        if (caller != null) {
          mv.visitVarInsn(ALOAD, 2);
          mv.visitVarInsn(ALOAD, 1);
          mv.visitTypeInsn(CHECKCAST, getInternalName(type));
          mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(type), method.getName(), getMethodDescriptor(method),
              false);
          mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(Encoder.class), caller.getMethod(), caller.getDescriptor(),
              false);
        } else {
          mv.visitVarInsn(ALOAD, 0);
          mv.visitFieldInsn(GETFIELD, className, field.getField().name(), getDescriptor(RecordInstanceWriter.class));
          mv.visitVarInsn(ALOAD, 1);
          mv.visitTypeInsn(CHECKCAST, getInternalName(type));
          mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(type), method.getName(), getMethodDescriptor(method),
              false);
          mv.visitVarInsn(ALOAD, 2);
          mv.visitVarInsn(ALOAD, 3);
          mv.visitMethodInsn(INVOKEINTERFACE, getInternalName(RecordInstanceWriter.class), "write", description, true);
        }
      }
      mv.visitInsn(RETURN);
      mv.visitMaxs(0, 0);
      mv.visitEnd();
    }

    cw.visitEnd();

    return cw.toByteArray();
  }
}
