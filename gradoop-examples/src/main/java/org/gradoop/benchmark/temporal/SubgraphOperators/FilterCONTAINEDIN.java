package org.gradoop.benchmark.temporal.SubgraphOperators;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.api.entities.EPGMElement;

public class FilterCONTAINEDIN<T extends EPGMElement> implements FilterFunction<T> {

  /**
   * Serial.
   */
  private static final long serialVersionUID = 1L;
  private Long lbVal;
  private Long ubVal;

  public FilterCONTAINEDIN(Long lbVal, Long ubVal) {
    this.lbVal = lbVal;
    this.ubVal = ubVal;
  }

  @Override
  public boolean filter(T arg0) throws Exception {
    Long tStart = arg0.getFrom();
    Long tEnd = arg0.getTo();

    return ((tStart >= lbVal) && (tEnd <= ubVal ));
  }

}
