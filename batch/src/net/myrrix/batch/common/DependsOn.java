/*
 * Copyright Myrrix Ltd
 */

package net.myrrix.batch.common;

import com.google.common.base.Preconditions;

/**
 * Encapsulates a dependency between two things, such that one must happen after the other, and depends on 
 * the first occurring first.
 * 
 * @author Sean Owen
 * @since 1.0
 * @param <T> the type of thing for which dependencies are expressed
 */
public final class DependsOn<T> {
  
  private final T happensNext;  
  private final T happensFirst;
  
  public DependsOn(T happensNext, T happensFirst) {
    Preconditions.checkNotNull(happensNext);    
    Preconditions.checkNotNull(happensFirst);
    this.happensNext = happensNext;
    this.happensFirst = happensFirst;    
  }
  
  public DependsOn(T happensFirst) {
    Preconditions.checkNotNull(happensFirst);
    this.happensNext = null;
    this.happensFirst = happensFirst;    
  }
  
  public static <T> DependsOn<T> nextAfterFirst(T happensNext, T happenstFirst) {
    return new DependsOn<T>(happensNext, happenstFirst);
  }
  
  public static <T> DependsOn<T> first(T happensFirst) {
    return new DependsOn<T>(happensFirst);
  }

  /**
   * @return thing that has to happen next. May be {@code null} if there is no prerequisite expressed.
   */
  T getHappensNext() {
    return happensNext;
  }
  
  /**
   * @return thing that has to happen first.
   */
  T getHappensFirst() {
    return happensFirst;
  }  
  
  @Override
  public String toString() {
    if (happensNext == null) {
      return "(" + happensFirst + ')';
    } else {
      return "(" + happensNext + " -> " + happensFirst + ')';      
    }
  }

}
