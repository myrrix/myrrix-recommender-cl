/*
 * Copyright Myrrix Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.myrrix.batch.similar;

import java.util.Iterator;

import org.apache.mahout.cf.taste.recommender.RecommendedItem;

import net.myrrix.common.LangUtils;
import net.myrrix.common.MutableRecommendedItem;
import net.myrrix.common.math.SimpleVectorMath;
import net.myrrix.common.collection.FastByIDMap;

final class MostSimilarItemIterator implements Iterator<RecommendedItem> {

  private final MutableRecommendedItem delegate;
  private final float[] itemFeatures;
  private final double itemFeaturesNorm;
  private final Iterator<FastByIDMap.MapEntry<float[]>> Yiterator;
  private final long toItemID;

  MostSimilarItemIterator(Iterator<FastByIDMap.MapEntry<float[]>> Yiterator,
                          long toItemID,
                          float[] itemFeatures) {
    delegate = new MutableRecommendedItem();
    this.toItemID = toItemID;
    this.itemFeatures = itemFeatures;
    this.itemFeaturesNorm = SimpleVectorMath.norm(itemFeatures);
    this.Yiterator = Yiterator;
  }

  @Override
  public boolean hasNext() {
    return Yiterator.hasNext();
  }

  @Override
  public RecommendedItem next() {
    FastByIDMap.MapEntry<float[]> entry = Yiterator.next();
    long itemID = entry.getKey();
    if (toItemID == itemID) {
      return null;
    }
    float[] candidateFeatures = entry.getValue();
    double candidateFeaturesNorm = SimpleVectorMath.norm(candidateFeatures);
    double similarity =
        SimpleVectorMath.dot(itemFeatures, candidateFeatures) / (itemFeaturesNorm * candidateFeaturesNorm);
    if (!LangUtils.isFinite(similarity)) {
      return null;
    }
    delegate.set(itemID, (float) similarity);
    return delegate;
  }

  /**
   * @throws UnsupportedOperationException
   */
  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

}
