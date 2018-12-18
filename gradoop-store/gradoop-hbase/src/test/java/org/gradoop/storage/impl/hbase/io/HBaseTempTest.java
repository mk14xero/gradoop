package org.gradoop.storage.impl.hbase.io;


import com.google.common.collect.Lists;
import org.apache.commons.lang.NotImplementedException;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.config.GradoopConfig;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.util.AsciiGraphLoader;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.config.GradoopHBaseConfig;
import org.gradoop.storage.impl.hbase.GradoopHBaseTestBase;
import org.gradoop.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.storage.impl.hbase.api.VertexHandler;
import org.gradoop.storage.impl.hbase.factory.HBaseEPGMStoreFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

import static org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment;
import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateEPGMGraphElementCollections;
import static org.gradoop.storage.impl.hbase.GradoopHBaseTestBase.createEmptyEPGMStore;
import static org.gradoop.storage.impl.hbase.GradoopHBaseTestBase.openEPGMStore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class HBaseTempTest extends GradoopFlinkTestBase {

  /**
   * Global Flink config for sources and sinks
   */
  private final GradoopFlinkConfig flinkconfig = GradoopFlinkConfig.createConfig(getExecutionEnvironment());


  /**
   * A store with social network data
   */

  private HBaseEPGMStore tempStore;
  List<Vertex> vertices;
  List<Edge> edges;
  List<GraphHead> graphHeads;


  /**
   * Instantiate the EPGMStore with a prefix and persist cinema data
   */

  @Before
  public void setUp() throws IOException {


    tempStore = openEPGMStore("HBaseTempTest.");

    InputStream inputStream = GradoopTestUtils.class.getResourceAsStream("/data/gdl/temp_example.gdl");
    final AsciiGraphLoader<GraphHead, Vertex, Edge> loader = AsciiGraphLoader.fromStream(inputStream, flinkconfig);

    String from = "from";
    String to = "to";

    // transforming Vertices
    vertices = loader.getVertices().stream().map(v -> {
    if (v.hasProperty(from)){
        v.setFrom(v.getPropertyValue(from).getLong());}
    else v.setFrom(0L);
    if (v.hasProperty(to)){
      v.setTo(v.getPropertyValue(to).getLong());}
    else v.setTo(Long.MAX_VALUE);
    return v;})
    .collect(Collectors.toList());
    for (Vertex v : vertices) {
    tempStore.writeVertex(v);
    }

    // transforming Edges
    edges = loader.getEdges().stream().map(e -> {
      if (e.hasProperty(from)){
        e.setFrom(e.getPropertyValue(from).getLong());}
      else e.setFrom(0L);
      if (e.hasProperty(to)){
        e.setTo(e.getPropertyValue(to).getLong());}
      else e.setTo(Long.MAX_VALUE);
      return e;})
        .collect(Collectors.toList());
    for (Edge e : edges) {
      tempStore.writeEdge(e);
    }

    // transforming GraphHeads
    graphHeads = loader.getGraphHeads().stream().map(g -> {
      if (g.hasProperty(from)){
        g.setFrom(g.getPropertyValue(from).getLong());}
      else g.setFrom(0L);
      if (g.hasProperty(to)){
        g.setTo(g.getPropertyValue(to).getLong());}
      else g.setTo(Long.MAX_VALUE);
      return g;})
        .collect(Collectors.toList());
    for (GraphHead g : graphHeads) {
      tempStore.writeGraphHead(g);
    }

    tempStore.flush();
  }

  /**
   * Close the EPGMStore after each test
   */

  @After
  public void tearDown() throws IOException {
    if (tempStore != null) {
      tempStore.drop();
      tempStore.close();
    }
  }

  @Test
  public void Read_Test() throws Exception {

    // read social graph from HBase via EPGMDatabase
    HBaseDataSource hBaseDataSource = new HBaseDataSource(tempStore, flinkconfig);
    GraphCollection collection = hBaseDataSource.getGraphCollection();

    Collection<Vertex> actualVertices = Lists.newArrayList();
    Collection<Edge> actualEdges = Lists.newArrayList();
    Collection<GraphHead> actualGraphHeads = Lists.newArrayList();

    collection.getVertices().output(new LocalCollectionOutputFormat<>(actualVertices));
    collection.getEdges().output(new LocalCollectionOutputFormat<>(actualEdges));
    collection.getGraphHeads().output(new LocalCollectionOutputFormat<>(actualGraphHeads));

    getExecutionEnvironment().execute();

    // Überprüfung, ob actual mit expected übereinstimmt
    validateEPGMElementCollections(vertices, actualVertices);
    validateEPGMElementCollections(edges, actualEdges);
    validateEPGMElementCollections(graphHeads, actualGraphHeads);

  }

  /**
   * All Test
   */

  @Test
  public void Read_All_Test() throws Exception {
    // 1269991551597 = 30.03.2010 1:25 Uhr
    Long begin = 1269991551597L;

    // 1435688530000 = 30.06.2015 3:46 Uhr
    Long end = 1435688530000L;

    // read social graph from HBase via EPGMDatabase
    HBaseDataSource hBaseDataSource = new HBaseDataSource(tempStore, flinkconfig);
    hBaseDataSource.setTemps(begin, end, HBaseDataSource.Querytype.ALL);
    GraphCollection collection = hBaseDataSource.getGraphCollection();

    Collection<Vertex> actualVertices = Lists.newArrayList();
    Collection<Edge> actualEdges = Lists.newArrayList();
    Collection<GraphHead> actualGraphHeads = Lists.newArrayList();

    collection.getVertices().output(new LocalCollectionOutputFormat<>(actualVertices));
    collection.getEdges().output(new LocalCollectionOutputFormat<>(actualEdges));
    collection.getGraphHeads().output(new LocalCollectionOutputFormat<>(actualGraphHeads));

    getExecutionEnvironment().execute();

    // Überprüfung, ob actual mit expected übereinstimmt
    validateEPGMElementCollections(vertices, actualVertices);
    validateEPGMElementCollections(edges, actualEdges);
    validateEPGMElementCollections(graphHeads, actualGraphHeads);
  }

  /**
   * Created In Test
   */

  @Test
  public void Read_Created_In_Test() throws Exception {

    // 1269991551597 = 30.03.2010 1:25 Uhr
    Long begin = 1269991551597L;

    // 1435688530000 = 30.06.2015 3:46 Uhr
    Long end = 1435688530000L;

    final List<Vertex> expectedVertices = vertices.stream()
        .filter(v -> v.getFrom() >= begin)
        .filter(v -> v.getFrom() <= end)
        .collect(Collectors.toList());

    final List<Edge> expectedEdges = edges.stream()
        .filter(e -> e.getFrom() >= begin)
        .filter(e -> e.getFrom() <= end)
        .collect(Collectors.toList());

    final List<GraphHead> expectedGraphHeads = graphHeads.stream()
        .filter(g -> g.getFrom() >= begin)
        .filter(g -> g.getFrom() <= end)
        .collect(Collectors.toList());

    HBaseDataSource hBaseDataSource = new HBaseDataSource(tempStore, flinkconfig);
    hBaseDataSource.setTemps(begin, end, HBaseDataSource.Querytype.CREATED_IN);
    GraphCollection collection = hBaseDataSource.getGraphCollection();

    Collection<Vertex> actualVertices = Lists.newArrayList();
    Collection<Edge> actualEdges = Lists.newArrayList();
    Collection<GraphHead> actualGraphHeads = Lists.newArrayList();

    collection.getVertices().output(new LocalCollectionOutputFormat<>(actualVertices));
    collection.getEdges().output(new LocalCollectionOutputFormat<>(actualEdges));
    collection.getGraphHeads().output(new LocalCollectionOutputFormat<>(actualGraphHeads));

    getExecutionEnvironment().execute();

    // Überprüfung, ob actual mit expected übereinstimmt
    validateEPGMElementCollections(expectedVertices, actualVertices);
    validateEPGMElementCollections(expectedEdges, actualEdges);
    validateEPGMElementCollections(expectedGraphHeads, actualGraphHeads);

  }

  /**
   * Deleted In Test
   */

  @Test
  public void Read_Deleted_In_Test() throws Exception {
    // 1269991551597 = 30.03.2010 1:25 Uhr
    Long begin = 1269991551597L;

    // 1435688530000 = 30.06.2015 3:46 Uhr
    Long end = 1435688530000L;

    final List<Vertex> expectedVertices = vertices.stream()
        .filter(v -> v.getTo() >= begin)
        .filter(v -> v.getTo() <= end)
        .collect(Collectors.toList());

    final List<Edge> expectedEdges = edges.stream()
        .filter(e -> e.getTo() >= begin)
        .filter(e -> e.getTo() <= end)
        .collect(Collectors.toList());

    final List<GraphHead> expectedGraphHeads = graphHeads.stream()
        .filter(g -> g.getTo() >= begin)
        .filter(g -> g.getTo() <= end)
        .collect(Collectors.toList());

    HBaseDataSource hBaseDataSource = new HBaseDataSource(tempStore, flinkconfig);
    hBaseDataSource.setTemps(begin, end, HBaseDataSource.Querytype.DELETED_IN);
    GraphCollection collection = hBaseDataSource.getGraphCollection();

    Collection<Vertex> actualVertices = Lists.newArrayList();
    Collection<Edge> actualEdges = Lists.newArrayList();
    Collection<GraphHead> actualGraphHeads = Lists.newArrayList();

    collection.getVertices().output(new LocalCollectionOutputFormat<>(actualVertices));
    collection.getEdges().output(new LocalCollectionOutputFormat<>(actualEdges));
    collection.getGraphHeads().output(new LocalCollectionOutputFormat<>(actualGraphHeads));

    getExecutionEnvironment().execute();

    // Überprüfung, ob actual mit expected übereinstimmt
    validateEPGMElementCollections(expectedVertices, actualVertices);
    validateEPGMElementCollections(expectedEdges, actualEdges);
    validateEPGMElementCollections(expectedGraphHeads, actualGraphHeads);

  }

  /**
   * From To Test
   */

  @Test
  public void Read_From_To_Test() throws Exception {
    // 1269991551597 = 30.03.2010 1:25 Uhr
    Long begin = 1269991551597L;

    // 1435688530000 = 30.06.2015 3:46 Uhr
    Long end = 1435688530000L;


    final List<Vertex> expectedVertices = vertices.stream()
        .filter(v -> v.getFrom() > 0L)
        .filter(v -> v.getFrom() < end)
        .filter(v -> v.getTo() > begin)
        .collect(Collectors.toList());

    final List<Edge> expectedEdges = edges.stream()
        .filter(v -> v.getFrom() > 0L)
        .filter(e -> e.getFrom() < end)
        .filter(e -> e.getTo() > begin)
        .collect(Collectors.toList());

    final List<GraphHead> expectedGraphHeads = graphHeads.stream()
        .filter(v -> v.getFrom() > 0L)
        .filter(g -> g.getFrom() < end)
        .filter(g -> g.getTo() > begin)
        .collect(Collectors.toList());

    HBaseDataSource hBaseDataSource = new HBaseDataSource(tempStore, flinkconfig);
    hBaseDataSource.setTemps(begin, end, HBaseDataSource.Querytype.FROM_TO);
    GraphCollection collection = hBaseDataSource.getGraphCollection();

    Collection<Vertex> actualVertices = Lists.newArrayList();
    Collection<Edge> actualEdges = Lists.newArrayList();
    Collection<GraphHead> actualGraphHeads = Lists.newArrayList();

    collection.getVertices().output(new LocalCollectionOutputFormat<>(actualVertices));
    collection.getEdges().output(new LocalCollectionOutputFormat<>(actualEdges));
    collection.getGraphHeads().output(new LocalCollectionOutputFormat<>(actualGraphHeads));

    getExecutionEnvironment().execute();

    // Überprüfung, ob actual mit expected übereinstimmt
    validateEPGMElementCollections(expectedVertices, actualVertices);
    validateEPGMElementCollections(expectedEdges, actualEdges);
    validateEPGMElementCollections(expectedGraphHeads, actualGraphHeads);
  }

  /**
   * Contained In Test
   */

  @Test
  public void Read_Contained_In_Test() throws Exception {
    // 1269991551597 = 30.03.2010 1:25 Uhr
    Long begin = 1269991551597L;

    // 1435688530000 = 30.06.2015 3:46 Uhr
    Long end = 1435688530000L;


    final List<Vertex> expectedVertices = vertices.stream()
        .filter(v -> v.getFrom() > 0L)
        .filter(v -> v.getFrom() >= begin)
        .filter(v -> v.getTo() <= end)
        .collect(Collectors.toList());

    final List<Edge> expectedEdges = edges.stream()
        .filter(v -> v.getFrom() > 0L)
        .filter(e -> e.getFrom() >= begin)
        .filter(e -> e.getTo() <= end)
        .collect(Collectors.toList());

    final List<GraphHead> expectedGraphHeads = graphHeads.stream()
        .filter(v -> v.getFrom() > 0L)
        .filter(g -> g.getFrom() >= begin)
        .filter(g -> g.getTo() <= end)
        .collect(Collectors.toList());

    HBaseDataSource hBaseDataSource = new HBaseDataSource(tempStore, flinkconfig);
    hBaseDataSource.setTemps(begin, end, HBaseDataSource.Querytype.CONTAINED_IN);
    GraphCollection collection = hBaseDataSource.getGraphCollection();

    Collection<Vertex> actualVertices = Lists.newArrayList();
    Collection<Edge> actualEdges = Lists.newArrayList();
    Collection<GraphHead> actualGraphHeads = Lists.newArrayList();

    collection.getVertices().output(new LocalCollectionOutputFormat<>(actualVertices));
    collection.getEdges().output(new LocalCollectionOutputFormat<>(actualEdges));
    collection.getGraphHeads().output(new LocalCollectionOutputFormat<>(actualGraphHeads));

    getExecutionEnvironment().execute();

    // Überprüfung, ob actual mit expected übereinstimmt
    validateEPGMElementCollections(expectedVertices, actualVertices);
    validateEPGMElementCollections(expectedEdges, actualEdges);
    validateEPGMElementCollections(expectedGraphHeads, actualGraphHeads);

  }

  /**
   * As Of Test
   */

  @Test
  public void Read_As_Of_Test() throws Exception {
    // 1269991551597 = 30.03.2010 1:25 Uhr
    Long begin = 1269991551597L;

    // 1435688530000 = 30.06.2015 3:46 Uhr
    Long end = 1435688530000L;

    final List<Vertex> expectedVertices = vertices.stream()
        .filter(v -> v.getFrom() > 0L)
        .filter(v -> v.getFrom() <= begin)
        .filter(v -> v.getTo() >= begin)
        .collect(Collectors.toList());

    final List<Edge> expectedEdges = edges.stream()
        .filter(v -> v.getFrom() > 0L)
        .filter(e -> e.getFrom() <= begin)
        .filter(e -> e.getTo() >= begin)
        .collect(Collectors.toList());

    final List<GraphHead> expectedGraphHeads = graphHeads.stream()
       .filter(v -> v.getFrom() > 0L)
        .filter(g -> g.getFrom() <= begin)
        .filter(g -> g.getTo() >= begin)
        .collect(Collectors.toList());

    HBaseDataSource hBaseDataSource = new HBaseDataSource(tempStore, flinkconfig);
    hBaseDataSource.setTemps(begin, end, HBaseDataSource.Querytype.AS_OF);
    GraphCollection collection = hBaseDataSource.getGraphCollection();

    Collection<Vertex> actualVertices = Lists.newArrayList();
    Collection<Edge> actualEdges = Lists.newArrayList();
    Collection<GraphHead> actualGraphHeads = Lists.newArrayList();

    collection.getVertices().output(new LocalCollectionOutputFormat<>(actualVertices));
    collection.getEdges().output(new LocalCollectionOutputFormat<>(actualEdges));
    collection.getGraphHeads().output(new LocalCollectionOutputFormat<>(actualGraphHeads));

    getExecutionEnvironment().execute();

    // Überprüfung, ob actual mit expected übereinstimmt
    validateEPGMElementCollections(expectedVertices, actualVertices);
    validateEPGMElementCollections(expectedEdges, actualEdges);
    validateEPGMElementCollections(expectedGraphHeads, actualGraphHeads);
  }

  /**
   * Between In Test
   */

  @Test
  public void Read_Between_In_Test() throws Exception {
    // 1269991551597 = 30.03.2010 1:25 Uhr
    Long begin = 1269991551597L;

    // 1435688530000 = 30.06.2015 3:46 Uhr
    Long end = 1435688530000L;


    final List<Vertex> expectedVertices = vertices.stream()
        .filter(v -> v.getFrom() > 0L)
        .filter(v -> v.getFrom() <= end)
        .filter(v -> v.getTo() > begin)
        .collect(Collectors.toList());

    final List<Edge> expectedEdges = edges.stream()
        .filter(v -> v.getFrom() > 0L)
        .filter(e -> e.getFrom() <= end)
        .filter(e -> e.getTo() > begin)
        .collect(Collectors.toList());

    final List<GraphHead> expectedGraphHeads = graphHeads.stream()
        .filter(v -> v.getFrom() > 0L)
        .filter(g -> g.getFrom() <= end)
        .filter(g -> g.getTo() > begin)
        .collect(Collectors.toList());

    HBaseDataSource hBaseDataSource = new HBaseDataSource(tempStore, flinkconfig);
    hBaseDataSource.setTemps(begin, end, HBaseDataSource.Querytype.BETWEEN_IN);
    GraphCollection collection = hBaseDataSource.getGraphCollection();

    Collection<Vertex> actualVertices = Lists.newArrayList();
    Collection<Edge> actualEdges = Lists.newArrayList();
    Collection<GraphHead> actualGraphHeads = Lists.newArrayList();

    collection.getVertices().output(new LocalCollectionOutputFormat<>(actualVertices));
    collection.getEdges().output(new LocalCollectionOutputFormat<>(actualEdges));
    collection.getGraphHeads().output(new LocalCollectionOutputFormat<>(actualGraphHeads));

    getExecutionEnvironment().execute();

    // Überprüfung, ob actual mit expected übereinstimmt
    validateEPGMElementCollections(expectedVertices, actualVertices);
    validateEPGMElementCollections(expectedEdges, actualEdges);
    validateEPGMElementCollections(expectedGraphHeads, actualGraphHeads);
  }

  /**
   * Valid During Test
   */

  @Test
  public void Read_Valid_During_Test() throws Exception {
    // 1269991551597 = 30.03.2010 1:25 Uhr
    Long begin = 1269991551597L;

    // 1435688530000 = 30.06.2015 3:46 Uhr
    Long end = 1435688530000L;


    final List<Vertex> expectedVertices = vertices.stream()
        .filter(v -> v.getFrom() > 0L)
        .filter(v -> v.getFrom() <= begin)
        .filter(v -> v.getTo() >= end)
        .collect(Collectors.toList());

    final List<Edge> expectedEdges = edges.stream()
        .filter(v -> v.getFrom() > 0L)
        .filter(e -> e.getFrom() <= begin)
        .filter(e -> e.getTo() >= end)
        .collect(Collectors.toList());

    final List<GraphHead> expectedGraphHeads = graphHeads.stream()
        .filter(v -> v.getFrom() > 0L)
        .filter(g -> g.getFrom() <= begin)
        .filter(g -> g.getTo() >= end)
        .collect(Collectors.toList());

    HBaseDataSource hBaseDataSource = new HBaseDataSource(tempStore, flinkconfig);
    hBaseDataSource.setTemps(begin, end, HBaseDataSource.Querytype.VALID_DURING);

    GraphCollection collection = hBaseDataSource.getGraphCollection();

    Collection<Vertex> actualVertices = Lists.newArrayList();
    Collection<Edge> actualEdges = Lists.newArrayList();
    Collection<GraphHead> actualGraphHeads = Lists.newArrayList();

    collection.getVertices().output(new LocalCollectionOutputFormat<>(actualVertices));
    collection.getEdges().output(new LocalCollectionOutputFormat<>(actualEdges));
    collection.getGraphHeads().output(new LocalCollectionOutputFormat<>(actualGraphHeads));

    getExecutionEnvironment().execute();

    // Überprüfung, ob actual mit expected übereinstimmt
    validateEPGMElementCollections(expectedVertices, actualVertices);
    validateEPGMElementCollections(expectedEdges, actualEdges);
    validateEPGMElementCollections(expectedGraphHeads, actualGraphHeads);
  }

@Test(expected = NotImplementedException.class)
  public void testWriteTempToSinkWithOverWrite() throws Exception {
    // Create an empty store
    HBaseEPGMStore tempStore = createEmptyEPGMStore("testWriteTempToSink");

    GradoopFlinkConfig flinkConfig = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    GraphCollection graphCollection = flinkConfig.getGraphCollectionFactory()
        .createEmptyCollection();

    new HBaseDataSink(tempStore, flinkConfig).write(graphCollection, true);

    getExecutionEnvironment().execute();
  }
}
