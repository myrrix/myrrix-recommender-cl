/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.merge;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.base.Splitter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.cf.taste.model.IDMigrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.myrrix.batch.common.AbstractMyrrixMapper;
import net.myrrix.batch.common.join.EntityJoinKey;
import net.myrrix.common.LangUtils;
import net.myrrix.common.OneWayMigrator;

/**
 * @author Sean Owen
 * @since 1.0
 */
public final class DelimitedInputMapper
    extends AbstractMyrrixMapper<LongWritable,Text,EntityJoinKey,EntityPrefWritable> {

  private static final Logger log = LoggerFactory.getLogger(DelimitedInputMapper.class);
  private static final Splitter DELIMITER = Splitter.on(',').trimResults();
  
  private final IDMigrator hash;
  private MultipleOutputs<?,?> tagOutput;
  private int badLineCount;

  public DelimitedInputMapper() {
    hash = new OneWayMigrator();
  }
  
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    tagOutput = new MultipleOutputs<EntityJoinKey,EntityPrefWritable>(context);
    badLineCount = 0;
  }

  @Override
  public void map(LongWritable position, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    if (line.isEmpty() || line.charAt(0) == '#') {
      return;
    }
    
    Iterator<String> it = DELIMITER.split(line).iterator();
    
    long userID;
    boolean userIsTag;
    long itemID;
    boolean itemIsTag;
    float pref;
    try {
      
      String userIDString = it.next();
      userIsTag = userIDString.startsWith("\"");
      if (userIsTag) {
        userID = hash.toLongID(userIDString.substring(1, userIDString.length() - 1));
      } else {
        userID = Long.parseLong(userIDString);
      }
      
      String itemIDString = it.next();
      itemIsTag = itemIDString.startsWith("\"");
      if (itemIsTag) {
        itemID = hash.toLongID(itemIDString.substring(1, itemIDString.length() - 1));
      } else {
        itemID = Long.parseLong(itemIDString);            
      }
      
      if (it.hasNext()) {
        String valueToken = it.next();
        pref = valueToken.isEmpty() ? Float.NaN : LangUtils.parseFloat(valueToken);
      } else {
        pref = 1.0f;
      }

    } catch (NoSuchElementException ignored) {
      logBadLine(line, context);
      return;
    } catch (IllegalArgumentException iae) { // includes NumberFormatException
      logBadLine(line, context);
      return;      
    }

    if (userIsTag && itemIsTag) {
      logBadLine(line, context);
      return;
    }
    
    if (userIsTag) {
      tagOutput.write("newItemTags", new LongWritable(userID), NullWritable.get(), "newItemTags/part");
    }
    if (itemIsTag) {
      tagOutput.write("newUserTags", new LongWritable(itemID), NullWritable.get(), "newUserTags/part");
    }
    
    context.write(new EntityJoinKey(userID, EntityJoinKey.AFTER), new EntityPrefWritable(itemID, pref));
  }
  
  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    tagOutput.close();
    super.cleanup(context);
  }

  private void logBadLine(String line, Context context) throws IOException {
    log.warn("Bad line; check counter IGNORED_BAD_LINES: '{}'", line);
    //context.getCounter(Counters.IGNORED_BAD_LINES).increment(1);
    if (++badLineCount > 100) { // Crude
      log.error("Too many bad input lines");
      throw new IOException("Too many bad lines, aborting");
    }
  }
  
  //private enum Counters {
  //  IGNORED_BAD_LINES,
  //}  

}