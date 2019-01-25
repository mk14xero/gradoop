package org.gradoop.benchmark.temporal.EPGMSubgraphOperators;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.api.entities.EPGMElement;

public class FilterASOF<T extends EPGMElement> implements FilterFunction<T> {

  /**
   * Serial.
   */
  private static final long serialVersionUID = 1L;
  private String propertyKeyFrom = null;
  private String propertyKeyTo = null;
  private long propertyValueFrom = 0;

  public FilterASOF(String propertyKeyFrom, String propertyKeyTo, long lbVal) {
    this.propertyKeyFrom = propertyKeyFrom;
    this.propertyKeyTo = propertyKeyTo;
    this.propertyValueFrom = lbVal;
  }

  @Override
  public boolean filter(T arg0) throws Exception {
    boolean isIncluded = false;

    Long tStart = arg0.getPropertyValue(propertyKeyFrom).getLong();
    Long tEnd = arg0.getPropertyValue(propertyKeyTo).getLong();

    if( (arg0.hasProperty(propertyKeyFrom) && tStart <= propertyValueFrom) &&
        (arg0.hasProperty(propertyKeyTo) && tEnd >= propertyValueFrom) ) {

      isIncluded = true;
    }

    return isIncluded;
  }

}
