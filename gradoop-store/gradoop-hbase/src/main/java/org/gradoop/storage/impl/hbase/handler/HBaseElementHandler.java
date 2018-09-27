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
package org.gradoop.storage.impl.hbase.handler;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;
import org.gradoop.storage.impl.hbase.api.ElementHandler;
import org.gradoop.storage.impl.hbase.constants.HBaseConstants;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Handler is used to write label and properties into HBase tables. This is
 * used by graphs and vertices.
 */
public abstract class HBaseElementHandler implements ElementHandler {
  /**
   * Byte representation of the meta column family.
   */
  static final byte[] CF_META_BYTES = Bytes.toBytes(HBaseConstants.CF_META);

  /**
   * Byte representation of the label column identifier.
   */
  private static final byte[] COL_LABEL_BYTES = Bytes.toBytes(HBaseConstants.COL_LABEL);

  /**
   * Byte representation of the property type column family.
   */
  private static final byte[] CF_PROPERTY_TYPE_BYTES =
    Bytes.toBytes(HBaseConstants.CF_PROPERTY_TYPE);

  /**
   * Byte representation of the property value column family.
   */
  private static final byte[] CF_PROPERTY_VALUE_BYTES =
    Bytes.toBytes(HBaseConstants.CF_PROPERTY_VALUE);

  private static final byte[] CF_TIMESTAMP_BYTES =
            Bytes.toBytes(HBaseConstants.CF_TS);

  private static final byte[] COL_TS_FROM_BYTES = Bytes.toBytes(HBaseConstants.COL_TS_FROM);

  private static final byte[] COL_TS_TO_BYTES = Bytes.toBytes(HBaseConstants.COL_TS_TO);

  /**
   * {@inheritDoc}
   */
  @Override
  public byte[] getRowKey(final GradoopId elementId, long from){
    byte[] gradoopId = elementId.toByteArray(); // 12 Byte
    byte[] underscore = "_".getBytes(); // 1 Byte ??
    byte[] fromBytes = ByteBuffer.allocate(Long.BYTES).putLong(from).array(); // 16 Byte

    return ByteBuffer.allocate(fromBytes.length + underscore.length + gradoopId.length).put(fromBytes).put(underscore).put(gradoopId).array();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopId getId(final byte[] rowKey) {
    if (rowKey == null) {
      throw new IllegalArgumentException("rowKey must not be null");
    }
        byte[] gradoopId = new byte[12];
        System.arraycopy(rowKey,17, gradoopId, 0, 12);

        return GradoopId.fromByteArray(gradoopId);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeLabel(final Put put, final EPGMElement entity) {
    return (entity.getLabel() == null) ? put :
      put.addColumn(CF_META_BYTES, COL_LABEL_BYTES, Bytes.toBytes(entity.getLabel()));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeProperty(final Put put, Property property) {
    byte[] type = PropertyValueUtils.Bytes.getTypeByte(property.getValue());
    byte[] bytesWithoutType = PropertyValueUtils.Bytes.getRawBytesWithoutType(property.getValue());
    put.addColumn(CF_PROPERTY_TYPE_BYTES, Bytes.toBytes(property.getKey()), type);
    put.addColumn(CF_PROPERTY_VALUE_BYTES, Bytes.toBytes(property.getKey()), bytesWithoutType);
    return put;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeProperties(final Put put, final EPGMElement entity) {
    if (entity.getProperties() != null && entity.getPropertyCount() > 0) {
      for (Property property : entity.getProperties()) {
        writeProperty(put, property);
      }
    }
    return put;
  }


  @Override
  public Put writeFrom(final Put put, final EPGMElement entity) {
      return (entity.getFrom() == null) ? put :
      put.addColumn(CF_TIMESTAMP_BYTES, COL_TS_FROM_BYTES, Bytes.toBytes(entity.getFrom()));
  }

    @Override
    public Put writeTo(final Put put, final EPGMElement entity) {
        return (entity.getTo() == null) ? put :
                put.addColumn(CF_TIMESTAMP_BYTES, COL_TS_TO_BYTES, Bytes.toBytes(entity.getTo()));
    }

  /**
   * {@inheritDoc}
   */
  @Override
  public String readLabel(final Result res) {
    return Bytes.toString(res.getValue(CF_META_BYTES, COL_LABEL_BYTES));
  }


  @Override
  public Long readFrom(final Result res) {
    return Bytes.toLong(res.getValue(CF_TIMESTAMP_BYTES, COL_TS_FROM_BYTES));
  }

  @Override
  public Long readTo(final Result res) {
    return Bytes.toLong(res.getValue(CF_TIMESTAMP_BYTES, COL_TS_TO_BYTES));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Properties readProperties(final Result res) {
    Properties properties = Properties.create();

    // Get Map<Qualifier, Value> which is Map<PropertyKey, TypeByte>
    Map<byte[], byte[]> typeFamilyMap = res.getFamilyMap(CF_PROPERTY_TYPE_BYTES);
    // Get Map<Qualifier, Value> which is Map<PropertyKey, ValueBytesWithoutType>
    Map<byte[], byte[]> valueFamilyMap = res.getFamilyMap(CF_PROPERTY_VALUE_BYTES);

    for (Map.Entry<byte[], byte[]> propertyColumn : typeFamilyMap.entrySet()) {
      properties.set(
        Bytes.toString(propertyColumn.getKey()),
        PropertyValueUtils.Bytes.createFromTypeValueBytes(
          propertyColumn.getValue(),
          valueFamilyMap.get(propertyColumn.getKey())));
    }

    return properties;
  }

  /**
   * Deserializes a gradoop id from HBase row key.
   *
   * @param res HBase row
   * @return gradoop id
   */
  GradoopId readId(Result res) {
    return GradoopId.fromByteArray(res.getRow());
  }
}
