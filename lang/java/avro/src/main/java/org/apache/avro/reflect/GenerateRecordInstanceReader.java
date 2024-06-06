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

import static org.objectweb.asm.Type.getConstructorDescriptor;
import static org.objectweb.asm.Type.getDescriptor;
import static org.objectweb.asm.Type.getInternalName;
import static org.objectweb.asm.Type.getMethodDescriptor;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.reflect.RecordFieldBuilder.FieldInfo;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;

public class GenerateRecordInstanceReader implements Opcodes {

  private static class Caller {

    private final String method;
    private final int save;
    private final int load;
    private final int width;

    public Caller(String method, int save, int load, int width) {
      super();
      this.method = method;
      this.save = save;
      this.load = load;
      this.width = width;
    }

    public String getMethod() {
      return method;
    }

    public int getSave() {
      return save;
    }

    public int getLoad() {
      return load;
    }

    public int getWidth() {
      return width;
    }
  }

  private static final Map<Class<?>, Caller> SIMPLE_READER = new HashMap<>();

  static {
    SIMPLE_READER.put(int.class, new Caller("readInt", ISTORE, ILOAD, 1));
    SIMPLE_READER.put(float.class, new Caller("readFloat", FSTORE, FLOAD, 1));
    SIMPLE_READER.put(boolean.class, new Caller("readBoolean", ISTORE, ILOAD, 1));
    SIMPLE_READER.put(double.class, new Caller("readDouble", DSTORE, DLOAD, 2));
    SIMPLE_READER.put(long.class, new Caller("readLong", LSTORE, LLOAD, 2));
    SIMPLE_READER.put(String.class, new Caller("readString", ASTORE, ALOAD, 1));
  }

  public RecordInstanceReader generate(List<RecordFieldBuilder.FieldInfo> fields, Constructor<?> constructor)
      throws ReflectiveOperationException {
    var type = constructor.getDeclaringClass();
    var packageName = "avro.generated." + type.getPackageName();
    final String fullClassName = packageName + "." + type.getSimpleName() + "Reader";

    var bytes = dump(fullClassName, fields, constructor);

    var loader = new ClassLoader(constructor.getDeclaringClass().getClassLoader()) {
      @Override
      public Class<?> findClass(String name) {
        return defineClass(name, bytes, 0, bytes.length);
      }
    };
    var clazz = loader.loadClass(fullClassName);
    var generatedConstructor = clazz.getConstructor(List.class);
    var instance = (RecordInstanceReader) generatedConstructor.newInstance(fields);

    return instance;
  }

  public static byte[] dump(String fullClassName, List<FieldInfo> fields, Constructor<?> constructor)
      throws ReflectiveOperationException {
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);

    String className = fullClassName.replace('.', '/');

    cw.visit(V17, ACC_PUBLIC + ACC_SUPER, className, null, getInternalName(Object.class),
        new String[] { getInternalName(RecordInstanceReader.class) });

    cw.visitSource(null, null);

    for (var field : fields) {
      if (!SIMPLE_READER.containsKey(field.getType())) {
        cw.visitField(ACC_PRIVATE + ACC_FINAL, field.getField().name(), getDescriptor(RecordInstanceReader.class), null,
            null);
      }
    }

    {
      var mv = cw.visitMethod(ACC_PUBLIC, "<init>", "(Ljava/util/List;)V",
          "(Ljava/util/List<Lorg/apache/avro/reflect/RecordInstanceReader;>;)V", null);
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
        mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(RecordFieldBuilder.FieldInfo.class), "getReader",
            getMethodDescriptor(RecordFieldBuilder.FieldInfo.class.getMethod("getReader")), false);
        mv.visitFieldInsn(PUTFIELD, className, field.getField().name(), getDescriptor(RecordInstanceReader.class));
      }
      mv.visitInsn(RETURN);
      mv.visitMaxs(0, 0);
      mv.visitEnd();
    }

    {
      var description = getMethodDescriptor(
          RecordInstanceReader.class.getMethod("read", ResolvingDecoder.class, ReflectDatumReader.class));
      var mv = cw.visitMethod(ACC_PUBLIC, "read", description, null,
          new String[] { getInternalName(IOException.class) });
      mv.visitCode();

      int index = 3;
      Map<FieldInfo, Integer> offsets = new HashMap<>();
      for (var field : fields) {
        offsets.put(field, index);
        var caller = SIMPLE_READER.get(field.getType());
        if (caller != null) {
          mv.visitVarInsn(ALOAD, 1);
          mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(ResolvingDecoder.class), caller.getMethod(),
              getMethodDescriptor(ResolvingDecoder.class.getMethod(caller.getMethod())), false);
          mv.visitVarInsn(caller.getSave(), index);
          index += caller.getWidth();

        } else {
          mv.visitVarInsn(ALOAD, 0);
          mv.visitFieldInsn(GETFIELD, className, field.getField().name(), getDescriptor(RecordInstanceReader.class));
          mv.visitVarInsn(ALOAD, 1);
          mv.visitVarInsn(ALOAD, 2);
          mv.visitMethodInsn(INVOKEINTERFACE, getInternalName(RecordInstanceReader.class), "read", description, true);
          mv.visitTypeInsn(CHECKCAST, getInternalName(field.getType()));
          mv.visitVarInsn(ASTORE, index++);
        }
      }

      mv.visitTypeInsn(NEW, getInternalName(constructor.getDeclaringClass()));
      mv.visitInsn(DUP);

      fields.stream().sorted(Comparator.comparing(FieldInfo::getConstructorOffset)).forEach(field -> {
        var caller = SIMPLE_READER.get(field.getType());

        if (caller != null) {
          mv.visitVarInsn(caller.getLoad(), offsets.get(field));
        } else {
          mv.visitVarInsn(ALOAD, offsets.get(field));
        }
      });

      mv.visitMethodInsn(INVOKESPECIAL, getInternalName(constructor.getDeclaringClass()), "<init>",
          getConstructorDescriptor(constructor), false);
      mv.visitInsn(ARETURN);
      mv.visitMaxs(0, 0);
      mv.visitEnd();
    }

    cw.visitEnd();

    return cw.toByteArray();
  }
}
