package org.gradoop.benchmark.temporal;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.statistics.VertexCount;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.config.GradoopHBaseConfig;
import org.gradoop.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.storage.impl.hbase.factory.HBaseEPGMStoreFactory;
import org.gradoop.storage.impl.hbase.io.HBaseDataSink;
import org.gradoop.storage.impl.hbase.io.HBaseDataSource;

import java.util.List;

public class TimestampDistribution {

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig conf = GradoopFlinkConfig.createConfig(env);

    HBaseEPGMStore graphStore = HBaseEPGMStoreFactory
        .createOrOpenEPGMStore(HBaseConfiguration.create(), GradoopHBaseConfig.getDefaultConfig(),
            args[0] + ".");

    HBaseDataSource hBaseDataSource = new HBaseDataSource(graphStore, conf);
    LogicalGraph graph = hBaseDataSource.getLogicalGraph();

    AggregateOperator<Tuple2<Long, Integer>> vertexsum = graph.getVertices().map(new MapFunction<Vertex, Tuple2<Long, Integer>>() {
      @Override
      public Tuple2<Long, Integer> map(Vertex vertex) throws Exception {
        return new Tuple2<>(vertex.getFrom() / 1000000000, 1);
      }
    }).groupBy(0).sum(1);

    AggregateOperator<Tuple2<Long, Integer>> edgesum = graph.getEdges().map(new MapFunction<Edge, Tuple2<Long, Integer>>() {
      @Override
      public Tuple2<Long, Integer> map(Edge edge) throws Exception {
        return new Tuple2<>(edge.getFrom() / 1000000000, 1);
      }
    }).groupBy(0).sum(1);

    AggregateOperator<Tuple2<Long, Integer>> vertexsum_to = graph.getVertices().map(new MapFunction<Vertex, Tuple2<Long, Integer>>() {
      @Override
      public Tuple2<Long, Integer> map(Vertex vertex) throws Exception {
        return new Tuple2<>(vertex.getTo() / 1000000000, 1);
      }
    }).groupBy(0).sum(1);

    AggregateOperator<Tuple2<Long, Integer>> edgesum_to = graph.getEdges().map(new MapFunction<Edge, Tuple2<Long, Integer>>() {
      @Override
      public Tuple2<Long, Integer> map(Edge edge) throws Exception {
        return new Tuple2<>(edge.getTo() / 1000000000, 1);
      }
    }).groupBy(0).sum(1);

    vertexsum.writeAsCsv("/home/mariakoemmpel/Desktop/vertexsum.csv");
    edgesum.writeAsCsv("/home/mariakoemmpel/Desktop/edgesum.csv");
    vertexsum_to.writeAsCsv("/home/mariakoemmpel/Desktop/vertexsum_to.csv");
    edgesum_to.writeAsCsv("/home/mariakoemmpel/Desktop/edgesum_to.csv");
    env.execute();

  }

}
