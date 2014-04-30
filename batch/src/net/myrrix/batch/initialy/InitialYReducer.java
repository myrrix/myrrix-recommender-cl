/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.initialy;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.mahout.math.VarLongWritable;

import net.myrrix.batch.common.AbstractMyrrixReducer;
import net.myrrix.batch.common.writable.FloatArrayWritable;
import net.myrrix.common.math.SimpleVectorMath;
import net.myrrix.common.random.RandomManager;
import net.myrrix.common.random.RandomUtils;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class InitialYReducer 
    extends AbstractMyrrixReducer<VarLongWritable,FloatArrayWritable,VarLongWritable,FloatArrayWritable> {

  private static final int MAX_FAR_FROM_VECTORS = 100000;
  
  private RandomGenerator random;
  private int features;
  private List<float[]> farFrom;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    random = RandomManager.getRandom();
    features = getConfiguration().getInt(InitialYStep.FEATURES_KEY, 0);
    Preconditions.checkArgument(features > 0, "Number of features must be positive");
    farFrom = Lists.newArrayList();    
  }

  @Override
  protected void reduce(VarLongWritable itemID,
                        Iterable<FloatArrayWritable> values,
                        Context context) throws IOException, InterruptedException {
    // Did we see any previous feature vector from previous Y?
    float[] featureVector = null;
    for (FloatArrayWritable value : values) {
      float[] maybeFeatureVector = value.get();
      int maybeLength = maybeFeatureVector.length;
      if (maybeLength == features) {
        // Only use this vector if not empty and has the right number of features
        featureVector = maybeFeatureVector;
        break;
      }
      if (maybeLength == 0) {
        continue;
      }
      if (maybeLength > features) {
        // Copy part of the existing vector
        featureVector = new float[features];
        System.arraycopy(maybeFeatureVector, 0, featureVector, 0, featureVector.length);
        SimpleVectorMath.normalize(featureVector);
        break;
      }
      if (maybeLength < features) {
        featureVector = new float[features];
        System.arraycopy(maybeFeatureVector, 0, featureVector, 0, maybeLength);
        for (int i = maybeLength; i < featureVector.length; i++) {
          featureVector[i] = (float) random.nextGaussian();
        }
        SimpleVectorMath.normalize(featureVector); 
        break;
      }
    }

    if (featureVector == null) {
      // No suitable prior vector, build a new one
      featureVector = RandomUtils.randomUnitVectorFarFrom(features, farFrom, random);
    }
    
    if (farFrom.size() < MAX_FAR_FROM_VECTORS) { // Simple cap to keep from getting too big
      farFrom.add(featureVector);
    }

    context.write(itemID, new FloatArrayWritable(featureVector));
  }
  
}
