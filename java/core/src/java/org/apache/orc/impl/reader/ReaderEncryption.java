/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.impl.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.HadoopShims;
import org.apache.orc.impl.HadoopShimsFactory;
import org.apache.orc.impl.MaskDescriptionImpl;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;

public class ReaderEncryption {
  private final HadoopShims.KeyProvider keyProvider;
  private final ReaderEncryptionKey[] keys;
  private final MaskDescriptionImpl[] masks;
  private final ReaderEncryptionVariant[] variants;
  // Mapping from each column to the next variant to try for that column.
  // A value of variants.length means no encryption
  private final int[] columnVariants;

  public ReaderEncryption() throws IOException {
    this(null, null, null, null, null);
  }

  public ReaderEncryption(OrcProto.Footer footer,
                          TypeDescription schema,
                          List<StripeInformation> stripes,
                          HadoopShims.KeyProvider provider,
                          Configuration conf) throws IOException {
    if (footer == null || !footer.hasEncryption()) {
      keyProvider = null;
      keys = new ReaderEncryptionKey[0];
      masks = new MaskDescriptionImpl[0];
      variants = new ReaderEncryptionVariant[0];
      columnVariants = null;
    } else {
      columnVariants = new int[schema.getMaximumId() + 1];
      keyProvider = provider != null ? provider :
          HadoopShimsFactory.get().getKeyProvider(conf, new SecureRandom());
      OrcProto.Encryption encrypt = footer.getEncryption();
      masks = new MaskDescriptionImpl[encrypt.getMaskCount()];
      for(int m=0; m < masks.length; ++m) {
        masks[m] = new MaskDescriptionImpl(m, encrypt.getMask(m));
      }
      keys = new ReaderEncryptionKey[encrypt.getKeyCount()];
      for(int k=0; k < keys.length; ++k) {
        keys[k] = new ReaderEncryptionKey(encrypt.getKey(k));
      }
      variants = new ReaderEncryptionVariant[encrypt.getVariantsCount()];
      for(int v=0; v < variants.length; ++v) {
        OrcProto.EncryptionVariant variant = encrypt.getVariants(v);
        variants[v] = new ReaderEncryptionVariant(keys[variant.getKey()], v,
            variant, schema, stripes, keyProvider);
      }
      Arrays.fill(columnVariants, variants.length);
      for(int v= variants.length - 1; v >= 0; --v) {
        ReaderEncryptionVariant variant = variants[v];
        TypeDescription column = variant.getRoot();
        for(int c=column.getId(); c <= column.getMaximumId(); ++c) {
          columnVariants[c] = v;
        }
      }
    }
  }

  public MaskDescriptionImpl[] getMasks() {
    return masks;
  }

  public ReaderEncryptionKey[] getKeys() {
    return keys;
  }

  public ReaderEncryptionVariant[] getVariants() {
    return variants;
  }

  /**
   * Do the selected columns have encryption?
   * @param included included[c] is true if c is included
   * @return true if we have encryption on at least one column
   */
  public boolean hasEncryption(boolean[] included) {
    if (keyProvider != null) {
      for(int c=0; c < included.length; ++c) {
        if (included[c] && columnVariants[c] < variants.length) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Get the variant for a given column that the user has access to.
   * If we haven't tried a given key, try to decrypt this variant's footer key
   * to see if the KeyProvider will give it to us. If not, continue to the
   * next variant.
   * @param column the column id
   * @return null for no encryption or the encryption variant
   */
  public ReaderEncryptionVariant getVariant(int column) throws IOException {
    if (keyProvider != null) {
      while (columnVariants[column] < variants.length) {
        ReaderEncryptionVariant result = variants[columnVariants[column]];
        switch (result.getKeyDescription().getState()) {
        case FAILURE:
          break;
        case SUCCESS:
          return result;
        case UNTRIED:
          // try to get the footer key, to see if we have access
          if (result.getFileFooterKey() != null) {
            return result;
          }
        }
        columnVariants[column] += 1;
      }
    }
    return null;
  }
}
