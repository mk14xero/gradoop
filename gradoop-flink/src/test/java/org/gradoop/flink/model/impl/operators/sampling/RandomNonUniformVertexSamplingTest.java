/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.sampling;

import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;

public class RandomNonUniformVertexSamplingTest extends ParametrizedTestForGraphSampling {

  public RandomNonUniformVertexSamplingTest(String testName, String seed, String sampleSize,
                                            String neighborType) {
    super(testName, seed, sampleSize, neighborType);
  }

  @Override
  public UnaryGraphToGraphOperator getSamplingOperator() {
    return new RandomNonUniformVertexSampling(sampleSize,seed);
  }
}