/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.recommend;

import java.util.Iterator;

import com.google.common.base.Preconditions;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;

import net.myrrix.common.MutableRecommendedItem;
import net.myrrix.common.collection.FastIDSet;
import net.myrrix.common.math.SimpleVectorMath;
import net.myrrix.common.collection.FastByIDMap;

/**
 * An {@link Iterator} that generates and iterates over all possible candidate items to recommend.
 * It is used to generate recommendations. The items with top values are taken as recommendations.
 *
 * @author Sean Owen
 */
final class RecommendIterator implements Iterator<RecommendedItem> {

  private final MutableRecommendedItem delegate;
  private final float[] features;
  private final Iterator<FastByIDMap.MapEntry<float[]>> Yiterator;
  private final FastIDSet knownItemIDs;

  RecommendIterator(float[] features, Iterator<FastByIDMap.MapEntry<float[]>> Yiterator, FastIDSet knownItemIDs) {
    Preconditions.checkArgument(features.length > 0, "Feature vector can't be empty");
    delegate = new MutableRecommendedItem();
    this.features = features;
    this.Yiterator = Yiterator;
    this.knownItemIDs = knownItemIDs;
  }

  @Override
  public boolean hasNext() {
    return Yiterator.hasNext();
  }

  @Override
  public RecommendedItem next() {
    FastByIDMap.MapEntry<float[]> entry = Yiterator.next();
    long itemID = entry.getKey();
    if (knownItemIDs.contains(itemID)) {
      return null;
    }
    delegate.set(itemID, (float) SimpleVectorMath.dot(entry.getValue(), features));
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
