/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.util.datasource;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.BlockValSet;

public final class ArrayBasedSingleValueSet implements BlockValSet {

  private int[] values;

  public ArrayBasedSingleValueSet(int[] values) {
    this.values = values;
  }


  @Override
  public BlockValIterator iterator() {

    return new ArrayBasedDocValIterator(values);
  }

  @Override
  public DataType getValueType() {
    return DataType.INT;
  }

  @Override
  public void readIntValues(int[] inDocIds, int inStartPos, int inDocIdsSize, int[] outDictionaryIds, int outStartPos) {
    int endPos = inStartPos + inDocIdsSize;
    for (int iter = inStartPos; iter < endPos; ++iter) {
      int row = inDocIds[iter];
      outDictionaryIds[outStartPos++] = values[row];
    }
  }
}