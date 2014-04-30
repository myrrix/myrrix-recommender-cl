/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.merge;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.util.FastMath;
import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.math.VarLongWritable;

import net.myrrix.batch.common.AbstractMyrrixReducer;
import net.myrrix.batch.common.join.EntityJoinKey;
import net.myrrix.common.LangUtils;
import net.myrrix.common.collection.FastByIDFloatMap;
import net.myrrix.common.collection.FastIDSet;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class MergeNewOldValuesReducer extends
    AbstractMyrrixReducer<EntityJoinKey,EntityPrefWritable,VarLongWritable,EntityPrefWritable> {

  private boolean doDecay;
  private float decayFactor;
  private float zeroThreshold;
  private Long previousUserID;
  private FastByIDFloatMap previousUserPrefs;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = getConfiguration();
    String decayFactorString = conf.get(MergeNewOldStep.MODEL_DECAY_FACTOR_KEY);
    if (decayFactorString == null) {
      decayFactor = 1.0f;
      doDecay = false;
    } else {
      decayFactor = LangUtils.parseFloat(decayFactorString);
      doDecay = true;
    }
    zeroThreshold = conf.getFloat(MergeNewOldStep.MODEL_DECAY_ZERO_THRESHOLD, 0.0f);
    Preconditions.checkArgument(zeroThreshold >= 0.0f, "Zero threshold must be nonnegative: %s", zeroThreshold);
  }

  @Override
  protected void reduce(EntityJoinKey key,
                        Iterable<EntityPrefWritable> itemPrefs,
                        Context context) throws IOException, InterruptedException {

    long currentUserID = key.getKey();

    if (key.getJoinOrder() == EntityJoinKey.BEFORE) {

      // Last old data had no match, just output it
      if (previousUserPrefs != null) {
        Preconditions.checkNotNull(previousUserID);
        output(previousUserID, previousUserPrefs, null, null, context);
        previousUserPrefs = null;
        previousUserID = null;
      }

      FastByIDFloatMap oldPrefs = new FastByIDFloatMap();
      for (EntityPrefWritable itemPref : itemPrefs) {
        float oldPrefValue = itemPref.getPrefValue();
        Preconditions.checkState(!Float.isNaN(oldPrefValue), "No prior pref value?");
        // Apply decay factor here, if applicable:
        oldPrefs.increment(itemPref.getID(), doDecay ? oldPrefValue * decayFactor : oldPrefValue);
      }

      previousUserPrefs = oldPrefs;
      previousUserID = currentUserID;

    } else {

      // Last old data had no match, just output it
      if (previousUserPrefs != null && currentUserID != previousUserID) {
        Preconditions.checkNotNull(previousUserID);
        output(previousUserID, previousUserPrefs, null, null, context);
        previousUserPrefs = null;
        previousUserID = null;
      }

      FastByIDFloatMap newPrefs = new FastByIDFloatMap();
      FastIDSet removedItemIDs = new FastIDSet();
      for (EntityPrefWritable itemPref : itemPrefs) {
        long itemID = itemPref.getID();
        float newPrefValue = itemPref.getPrefValue();
        if (Float.isNaN(newPrefValue)) {
          removedItemIDs.add(itemID);
        } else {
          newPrefs.increment(itemID, newPrefValue);
        }
      }

      output(currentUserID, previousUserPrefs, newPrefs, removedItemIDs, context);

      previousUserPrefs = null;
      previousUserID = null;

    }

  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException  {
    if (previousUserPrefs != null) {
      Preconditions.checkNotNull(previousUserID);
      output(previousUserID, previousUserPrefs, null, null, context);
    }
    super.cleanup(context);
  }

  private void output(long userID,
                      FastByIDFloatMap oldPrefs,
                      FastByIDFloatMap newPrefs,
                      FastIDSet removedItemIDs,
                      Context context) throws IOException, InterruptedException {

    VarLongWritable outKey = new VarLongWritable(userID);

    // Old prefs may be null when there is no previous generation, for example, or the user is new.
    // First, write out existing prefs, possibly updated by new values
    if (oldPrefs != null) {
      for (FastByIDFloatMap.MapEntry entry : oldPrefs.entrySet()) {
        long itemID = entry.getKey();
        float oldPrefValue = entry.getValue();
        Preconditions.checkState(!Float.isNaN(oldPrefValue), "No prior pref value?");

        // May be NaN if no new data at all, or new data has no update:
        float sum = oldPrefValue;
        if (newPrefs != null) {
          float newPrefValue = newPrefs.get(itemID);
          if (!Float.isNaN(newPrefValue)) {
            sum += newPrefValue;
          }
        }

        boolean remove = false;
        if (removedItemIDs != null && removedItemIDs.contains(itemID)) {
          remove = true;
          //context.getCounter(Counters.REMOVED_ITEMS).increment(1);
        } else if (FastMath.abs(sum) <= zeroThreshold) {
          remove = true;
          //context.getCounter(Counters.DECAYED_ITEMS).increment(1);
        }

        if (!remove) {
          context.write(outKey, new EntityPrefWritable(itemID, sum));
        }
      }
    }

    // Now output new data, that didn't exist in old prefs
    if (newPrefs != null) {
      for (FastByIDFloatMap.MapEntry entry : newPrefs.entrySet()) {
        long itemID = entry.getKey();
        if (oldPrefs == null || !oldPrefs.containsKey(itemID)) {
          //context.getCounter(Counters.NEW_ITEMS).increment(1);
          // It wasn't already written. If it exists in newPrefs, it's also not removed
          float newPrefValue = entry.getValue();
          if (FastMath.abs(newPrefValue) > zeroThreshold) {
            context.write(outKey, new EntityPrefWritable(itemID, newPrefValue));
          //} else {
          //  context.getCounter(Counters.DECAYED_ITEMS).increment(1);
          }
        }
      }
    }

  }

  //private enum Counters {
  //  REMOVED_ITEMS,
  //  DECAYED_ITEMS,
  //  NEW_ITEMS,
  //}

}
