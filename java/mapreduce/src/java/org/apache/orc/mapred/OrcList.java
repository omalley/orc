/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc.mapred;

import org.apache.hadoop.io.Writable;
import org.apache.orc.TypeDescription;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * An ArrayList implementation that implements Writable.
 * @param <E> the element type, which must be Writable
 */
public class OrcList<E extends Writable>
    extends ArrayList<E> implements Writable {
  private final TypeDescription childSchema;

  public OrcList(TypeDescription schema) {
    childSchema = schema.getChildren().get(0);
  }

  public OrcList(TypeDescription schema, int initialCapacity) {
    super(initialCapacity);
    childSchema = schema.getChildren().get(0);
  }

  @Override
  public void write(DataOutput output) throws IOException {
    Iterator<E> itr = iterator();
    output.writeInt(size());
    while (itr.hasNext()) {
      E obj = itr.next();
      output.writeBoolean(obj != null);
      if (obj != null) {
        obj.write(output);
      }
    }
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    clear();
    int size = input.readInt();
    ensureCapacity(size);
    for(int i=0; i < size; ++i) {
      if (input.readBoolean()) {
        E obj = (E) OrcStruct.createValue(childSchema);
        obj.readFields(input);
        add(obj);
      } else {
        add(null);
      }
    }
  }
}
