package org.gradoop.benchmark.temporal.EPGMSubgraphOperators;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.api.entities.EPGMElement;

public class FilterFROM<T extends EPGMElement> implements FilterFunction<T> {

  /**
   * Serial.
   */
  private static final long serialVersionUID = 1L;
  private String propertyKeyFrom = null;
  private String propertyKeyTo = null;
  private long propertyValueFrom = 0;
  private long propertyValueTo = 0;

  public FilterFROM(String propertyKeyFrom, String propertyKeyTo, long propertyValueFrom, long propertyValueTo) {
    this.propertyKeyFrom = propertyKeyFrom;
    this.propertyKeyTo = propertyKeyTo;
    this.propertyValueFrom = propertyValueFrom;
    this.propertyValueTo = propertyValueTo;
  }

  @Override
  public boolean filter(T arg0) throws Exception {
    boolean isIncluded = false;

    Long tStart = arg0.getPropertyValue(propertyKeyFrom).getLong();
    Long tEnd = arg0.getPropertyValue(propertyKeyTo).getLong();

    if( (arg0.hasProperty(propertyKeyFrom) && tStart < propertyValueTo) &&
        (arg0.hasProperty(propertyKeyTo) && tEnd > propertyValueFrom) ) {

      isIncluded = true;
    }

    return isIncluded;
  }

}
