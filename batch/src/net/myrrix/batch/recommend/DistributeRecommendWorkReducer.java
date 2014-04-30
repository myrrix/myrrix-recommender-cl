/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.recommend;

import java.io.IOException;
import java.util.Iterator;

import com.google.common.base.Preconditions;
import org.apache.mahout.math.VarIntWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.batch.common.AbstractMyrrixReducer;
import net.myrrix.batch.common.join.EntityJoinKey;
import net.myrrix.common.collection.FastIDSet;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class DistributeRecommendWorkReducer
    extends AbstractMyrrixReducer<EntityJoinKey,FeaturesOrKnownIDsWritable,VarIntWritable,IDAndKnownItemsAndFloatArrayWritable> {

  private static final Logger log = LoggerFactory.getLogger(DistributeRecommendWorkReducer.class);

  private float[] currentUserFeatures;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    log.info("Distributing data to {} reducers", getNumPartitions()); // Assuming this matches next step's count
  }

  @Override
  public void reduce(EntityJoinKey key,
                     Iterable<FeaturesOrKnownIDsWritable> values,
                     Context context) throws IOException, InterruptedException {

    long userID = key.getKey();
    Iterator<FeaturesOrKnownIDsWritable> it = values.iterator();
    FeaturesOrKnownIDsWritable featuresOrItems = it.next();
    Preconditions.checkState(!it.hasNext(), "Multiple values for ID %s", userID);

    if (key.getJoinOrder() == EntityJoinKey.BEFORE) {
      // Features
      Preconditions.checkState(currentUserFeatures == null, "Already had features for key");
      currentUserFeatures = featuresOrItems.getFeatures();
    } else {
      // Known item IDs
      Preconditions.checkNotNull(currentUserFeatures, "No features for key");
      FastIDSet knownItemIDs = featuresOrItems.getKnownIDs();
      Preconditions.checkNotNull(knownItemIDs, "No known IDs");
      float[] theCurrentUserFeatures = currentUserFeatures;
      VarIntWritable outKey = new VarIntWritable();
      IDAndKnownItemsAndFloatArrayWritable outValue =
          new IDAndKnownItemsAndFloatArrayWritable(userID, theCurrentUserFeatures, knownItemIDs);
      int numReducers = getNumPartitions();
      for (int i = 0; i < numReducers; i++) {
        outKey.set(i);
        context.write(outKey, outValue);
      }
      currentUserFeatures = null;
    }

  }

}
