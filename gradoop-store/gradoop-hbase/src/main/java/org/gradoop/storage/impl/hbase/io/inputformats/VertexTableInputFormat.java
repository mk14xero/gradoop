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
package org.gradoop.storage.impl.hbase.io.inputformats;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.storage.common.api.EPGMGraphOutput;
import org.gradoop.storage.impl.hbase.api.VertexHandler;
import org.gradoop.storage.impl.hbase.io.HBaseDataSource;

import static org.gradoop.storage.impl.hbase.constants.HBaseConstants.CF_TS;
import static org.gradoop.storage.impl.hbase.constants.HBaseConstants.COL_TS_TO;

/**
 * Reads vertex data from HBase.
 */
public class VertexTableInputFormat extends BaseTableInputFormat<Vertex> {

  /**
   * Handles reading of persistent vertex data.
   */
  private final VertexHandler vertexHandler;

  /**
   * Table to read from.
   */
  private final String vertexTableName;

  /**
   * Creates an vertex table input format.
   *
   * @param vertexHandler   vertex data handler
   * @param vertexTableName vertex data table name
   */
  public VertexTableInputFormat(VertexHandler vertexHandler, String vertexTableName) {
    this.vertexHandler = vertexHandler;
    this.vertexTableName = vertexTableName;
  }

  /**
   * Get the scanner instance. If a query was applied to the elementHandler,
   * the Scan will be extended with a HBase filter representation of that query.
   *
   * @return the Scan instance with an optional HBase filter applied
   */
  @Override
  protected Scan getScanner() {
    Scan scan = new Scan();
    scan.setCaching(EPGMGraphOutput.DEFAULT_CACHE_SIZE);

    if (vertexHandler.getQuery() != null) {
      attachFilter(vertexHandler.getQuery(), scan);
    }

    switch (type){
      case AS_OF:
        if (begin != null) {
          scan.withStartRow(Bytes.toBytes(1L))
              .withStopRow(Bytes.toBytes(begin), true)
              .setFilter(new SingleColumnValueFilter(Bytes.toBytesBinary(CF_TS),
                  Bytes.toBytesBinary(COL_TS_TO),
                  CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(begin)));
        }
        break;

      case BETWEEN_IN:
        if(begin != null && end != null) {
          scan.withStartRow(Bytes.toBytes(1L))
              .withStopRow(Bytes.toBytes(end), true);
          scan.setFilter(new SingleColumnValueFilter(Bytes.toBytesBinary(CF_TS),
              Bytes.toBytesBinary(COL_TS_TO),
              CompareFilter.CompareOp.GREATER, Bytes.toBytes(begin)));
        }
        break;

      case CONTAINED_IN:
        if (begin != null && end != null) {
          scan.withStartRow(Bytes.toBytes(begin), true)
              .setFilter(new SingleColumnValueFilter(Bytes.toBytesBinary(CF_TS),
                  Bytes.toBytesBinary(COL_TS_TO),
                  CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(end)));
        }
        break;

      case FROM_TO:
        if (begin != null && end != null) {
          scan.withStartRow(Bytes.toBytes(1L))
              .withStopRow(Bytes.toBytes(end))
              .setFilter(new SingleColumnValueFilter(Bytes.toBytesBinary(CF_TS),
                  Bytes.toBytesBinary(COL_TS_TO),
                  CompareFilter.CompareOp.GREATER, Bytes.toBytes(begin)));
        }
        break;

      case VALID_DURING:
        if (begin != null && end != null) {
          scan.withStartRow(Bytes.toBytes(1L))
              .withStopRow(Bytes.toBytes(begin), true)
              .setFilter(new SingleColumnValueFilter(Bytes.toBytesBinary(CF_TS),
                  Bytes.toBytesBinary(COL_TS_TO),
                  CompareFilter.CompareOp.GREATER_OR_EQUAL,
                  Bytes.toBytes(end)));
        }
        break;

      case CREATED_IN:
        if (begin != null && end != null) {
          scan.withStartRow(Bytes.toBytes(begin), true)
              .withStopRow(Bytes.toBytes(end), true);
        }
        break;

      case DELETED_IN:
        if (begin != null && end != null) {
          FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
          allFilters.addFilter(new SingleColumnValueFilter(Bytes.toBytesBinary(CF_TS),
              Bytes.toBytesBinary(COL_TS_TO),
              CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(begin)));
          allFilters.addFilter(new SingleColumnValueFilter(Bytes.toBytesBinary(CF_TS),
              Bytes.toBytesBinary(COL_TS_TO),
              CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(end)));
          scan.setFilter(allFilters);
        }
        break;

      case ALL:
        break;
    }

    return scan;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected String getTableName() {
    return vertexTableName;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Tuple1<Vertex> mapResultToTuple(Result result) {
    return new Tuple1<>(vertexHandler.readVertex(result));
  }
}
