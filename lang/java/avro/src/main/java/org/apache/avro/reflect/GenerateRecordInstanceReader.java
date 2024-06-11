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

import org.apache.avro.Schema;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.reflect.RecordFieldBuilder.FieldInfo;
import org.apache.avro.reflect.RecordFieldBuilder.LookupKey;
import org.apache.avro.specific.SpecificData;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

class GenerateRecordInstanceReader implements Opcodes {

  private static final Map<Class<?>, PrimitiveInfo> PRIMITIVES = new HashMap<>();
  private static final int THIS = 0;
  private static final int LIST = 1;
  private static final int DECODER = 1;
  private static final int READER = 2;

  static {
    PRIMITIVES.put(int.class, new PrimitiveInfo(ISTORE, ILOAD, 1));
    PRIMITIVES.put(float.class, new PrimitiveInfo(FSTORE, FLOAD, 1));
    PRIMITIVES.put(boolean.class, new PrimitiveInfo(ISTORE, ILOAD, 1));
    PRIMITIVES.put(double.class, new PrimitiveInfo(DSTORE, DLOAD, 2));
    PRIMITIVES.put(long.class, new PrimitiveInfo(LSTORE, LLOAD, 2));
    PRIMITIVES.put(short.class, new PrimitiveInfo(ISTORE, ILOAD, 1));
    PRIMITIVES.put(char.class, new PrimitiveInfo(ISTORE, ILOAD, 1));
    PRIMITIVES.put(byte.class, new PrimitiveInfo(ISTORE, ILOAD, 1));

  }

  private static final Map<LookupKey, String> SIMPLE_READER = new HashMap<>();

  static {
    SIMPLE_READER.put(new LookupKey(int.class, Schema.Type.INT), "readInt");
    SIMPLE_READER.put(new LookupKey(float.class, Schema.Type.FLOAT), "readFloat");
    SIMPLE_READER.put(new LookupKey(boolean.class, Schema.Type.BOOLEAN), "readBoolean");
    SIMPLE_READER.put(new LookupKey(double.class, Schema.Type.DOUBLE), "readDouble");
    SIMPLE_READER.put(new LookupKey(long.class, Schema.Type.LONG), "readLong");
    SIMPLE_READER.put(new LookupKey(String.class, Schema.Type.STRING), "readString");
  }

  RecordInstanceReader generate(List<FieldInfo> fields, Constructor<?> constructor)
      throws ReflectiveOperationException {
    var type = constructor.getDeclaringClass();
    // store as a static class within the class to remove visibility issues
    final String fullClassName = type.getName() + "$GeneratedAvroReader";

    var bytes = generateClass(fullClassName, fields, constructor);

    var loader = new ClassLoader(constructor.getDeclaringClass().getClassLoader()) {
      @Override
      public Class<?> findClass(String name) {
        return defineClass(name, bytes, 0, bytes.length);
      }
    };
    var clazz = loader.loadClass(fullClassName);
    var generatedConstructor = clazz.getConstructor(List.class);

    return (RecordInstanceReader) generatedConstructor.newInstance(fields);
  }

  static byte[] generateClass(String fullClassName, List<FieldInfo> fields, Constructor<?> constructor)
      throws ReflectiveOperationException {
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);

    String className = fullClassName.replace('.', '/');

    cw.visit(V17, ACC_PUBLIC + ACC_SUPER + ACC_STATIC, className, null, getInternalName(Object.class),
        new String[] { getInternalName(RecordInstanceReader.class) });

    cw.visitSource(null, null);

    generateFields(fields, cw);

    generateConstructor(fields, cw, className);

    generateReader(fields, constructor, cw, className);

    cw.visitEnd();

    return cw.toByteArray();
  }

  private static void generateFields(List<FieldInfo> fields, ClassWriter cw) {
    for (var field : fields) {
      var lookup = new LookupKey(field);

      if (!SIMPLE_READER.containsKey(lookup)) {
        cw.visitField(ACC_PRIVATE + ACC_FINAL, field.getName(), getDescriptor(RecordInstanceReader.class), null, null);
      }
    }
  }

  private static void generateConstructor(List<FieldInfo> fields, ClassWriter cw, String className)
      throws NoSuchMethodException {
    var mv = cw.visitMethod(ACC_PUBLIC, "<init>", "(Ljava/util/List;)V",
        "(Ljava/util/List<Lorg/apache/avro/reflect/RecordInstanceReader;>;)V", null);
    mv.visitVarInsn(ALOAD, THIS);
    mv.visitMethodInsn(INVOKESPECIAL, getInternalName(Object.class), "<init>", "()V", false);

    for (int i = 0; i < fields.size(); i++) {
      var field = fields.get(i);
      var lookup = new LookupKey(field);

      if (SIMPLE_READER.containsKey(lookup)) {
        continue;
      }
      mv.visitVarInsn(ALOAD, THIS);
      mv.visitVarInsn(ALOAD, LIST);
      mv.visitIntInsn(SIPUSH, i);
      mv.visitMethodInsn(INVOKEINTERFACE, getInternalName(List.class), "get",
          getMethodDescriptor(List.class.getMethod("get", int.class)), true);
      mv.visitTypeInsn(CHECKCAST, getInternalName(FieldInfo.class));
      mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(FieldInfo.class), "getReader",
          getMethodDescriptor(FieldInfo.class.getMethod("getReader")), false);
      mv.visitFieldInsn(PUTFIELD, className, field.getName(), getDescriptor(RecordInstanceReader.class));
    }
    mv.visitInsn(RETURN);
    mv.visitMaxs(0, 0);
    mv.visitEnd();
  }

  private static void generateReader(List<FieldInfo> fields, Constructor<?> constructor, ClassWriter cw,
      String className) throws NoSuchMethodException {
    var description = getMethodDescriptor(
        RecordInstanceReader.class.getMethod("read", ResolvingDecoder.class, ReflectDatumReader.class));
    var mv = cw.visitMethod(ACC_PUBLIC, "read", description, null, new String[] { getInternalName(IOException.class) });
    mv.visitCode();

    int index = 3;
    Map<FieldInfo, Integer> offsets = new HashMap<>();

    // call field readers and assign to local variables
    for (var field : fields) {
      var lookup = new LookupKey(field);

      offsets.put(field, index);
      var caller = SIMPLE_READER.get(lookup);
      if (caller != null) {
        mv.visitVarInsn(ALOAD, DECODER);
        mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(ResolvingDecoder.class), caller,
            getMethodDescriptor(ResolvingDecoder.class.getMethod(caller)), false);
      } else {
        mv.visitVarInsn(ALOAD, THIS);
        mv.visitFieldInsn(GETFIELD, className, field.getName(), getDescriptor(RecordInstanceReader.class));
        mv.visitVarInsn(ALOAD, DECODER);
        mv.visitVarInsn(ALOAD, READER);
        mv.visitMethodInsn(INVOKEINTERFACE, getInternalName(RecordInstanceReader.class), "read", description, true);
        if (field.getType().isPrimitive()) {
          castPrimitive(field, mv);
        }
      }
      if (field.getType().isPrimitive()) {
        var primitive = PRIMITIVES.get(field.getType());
        mv.visitVarInsn(primitive.getSave(), index);
        index += primitive.getWidth();
      } else {
        mv.visitTypeInsn(CHECKCAST, getInternalName(field.getType()));
        mv.visitVarInsn(ASTORE, index++);
      }
    }

    // create record and load in local variables
    mv.visitTypeInsn(NEW, getInternalName(constructor.getDeclaringClass()));
    mv.visitInsn(DUP);

    fields.stream().sorted(Comparator.comparing(FieldInfo::getConstructorOffset)).forEach(field -> {
      if (field.getType().isPrimitive()) {
        mv.visitVarInsn(PRIMITIVES.get(field.getType()).getLoad(), offsets.get(field));
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

  private static void castPrimitive(FieldInfo field, MethodVisitor mv) throws NoSuchMethodException {

    var fType = field.getType();
    if (field.getSchema() != null) {
      var writtenType = ReflectData.getClassProp(field.getSchema(), SpecificData.CLASS_PROP);
      if (writtenType != null) {
        fType = writtenType;
      } else {
        var type = field.getSchemaType();
        if (type == Schema.Type.BOOLEAN) {
          fType = boolean.class;
        } else if (type == Schema.Type.INT) {
          fType = int.class;
        } else if (type == Schema.Type.DOUBLE) {
          fType = double.class;
        } else if (type == Schema.Type.LONG) {
          fType = long.class;
        } else if (type == Schema.Type.FLOAT) {
          fType = float.class;
        }
      }
    }
    if (fType.equals(double.class) || fType.equals(Double.class)) {
      fType = double.class;
      addPrimitiveCast(mv, Double.class, "double");
    } else if (fType.equals(float.class) || fType.equals(Float.class)) {
      fType = float.class;
      addPrimitiveCast(mv, Float.class, "float");
    } else if (fType.equals(long.class) || fType.equals(Long.class)) {
      fType = long.class;
      addPrimitiveCast(mv, Long.class, "long");
    } else if (fType.equals(boolean.class) || fType.equals(Boolean.class)) {
      fType = boolean.class;
      addPrimitiveCast(mv, Boolean.class, "boolean");
    } else if (fType.equals(int.class) || fType.equals(Integer.class)) {
      fType = int.class;
      addPrimitiveCast(mv, Integer.class, "int");
    } else if (fType.equals(byte.class) || fType.equals(Byte.class)) {
      fType = byte.class;
      addPrimitiveCast(mv, Byte.class, "byte");
    } else if (fType.equals(char.class) || fType.equals(Character.class)) {
      fType = char.class;

      addPrimitiveCast(mv, Character.class, "char");
    } else if (fType.equals(short.class) || fType.equals(Short.class)) {
      fType = short.class;
      addPrimitiveCast(mv, Short.class, "short");
    }

    if (!fType.equals(field.getType())) {
      var target = field.getType();
      boolean intRepresentation = fType.equals(int.class) || fType.equals(short.class) || fType.equals(char.class)
          || fType.equals(byte.class);
      if (target == double.class) {
        if (fType.equals(long.class)) {
          mv.visitInsn(L2D);
          return;
        }
        if (fType.equals(float.class)) {
          mv.visitInsn(F2D);
          return;
        }
        if (intRepresentation) {
          mv.visitInsn(I2D);
          return;
        }
      } else if (target == float.class) {
        if (intRepresentation) {
          mv.visitInsn(I2F);
          return;
        }
      } else if (target == long.class) {
        if (intRepresentation) {
          mv.visitInsn(I2L);
          return;
        }
      } else if (target == int.class) {
        if (fType.equals(short.class)) {
          return;
        }
      } else if (target == short.class) {
        if (fType.equals(byte.class)) {
          return;
        }
      }
      throw new IllegalArgumentException("Record and Schema don't having matching type for field " + field.getName()
          + " Written with type " + fType + " record expects " + target);
    }

  }

  private static void addPrimitiveCast(MethodVisitor mv, Class<?> type, String primitiveType)
      throws NoSuchMethodException {
    var name = primitiveType + "Value";
    mv.visitTypeInsn(CHECKCAST, getInternalName(type));
    mv.visitMethodInsn(INVOKEVIRTUAL, getInternalName(type), name, getMethodDescriptor(type.getMethod(name)), false);
  }

  private static class PrimitiveInfo {

    private final int save;
    private final int load;
    private final int width;

    public PrimitiveInfo(int save, int load, int width) {
      this.save = save;
      this.load = load;
      this.width = width;
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
}
