package org.gradoop.benchmark.temporal;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.config.GradoopHBaseConfig;
import org.gradoop.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.storage.impl.hbase.factory.HBaseEPGMStoreFactory;
import org.gradoop.storage.impl.hbase.io.HBaseDataSink;

public class WriteGraphToHBase {

  /**
   *
   * @param args args[0] -> Path to graph
   *             args[1] -> hbase table prefix without '.'
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig conf = GradoopFlinkConfig.createConfig(env);

      DataSource source = new CSVDataSource(args[0], conf);
      LogicalGraph graph = source.getLogicalGraph();
      graph = transformGraph(graph);

      // write graph to HBase
      HBaseEPGMStore graphStore = HBaseEPGMStoreFactory.createOrOpenEPGMStore(HBaseConfiguration.create(), GradoopHBaseConfig.getDefaultConfig(), args[1] + ".");
      DataSink hBaseSink = new HBaseDataSink(graphStore, conf);
      hBaseSink.write(graph);

      env.execute();
  }

  /**
   * Transforms the Graph from CSV format into Dataset format
   */
  private static LogicalGraph transformGraph(LogicalGraph graph){

    graph = graph.transform(TransformationFunction.keep()

        , new TransformationFunction<Vertex>() {
          public Vertex apply(Vertex vertex, Vertex el1) {

            String birthday = "birthday";
            String founded = "founded";


            if (vertex.hasProperty(birthday)) {
              vertex.setFrom(vertex.getPropertyValue(birthday).getLong());
              vertex.setTo(vertex.getPropertyValue(birthday).getLong() + (3602220L*24L*23L));
            }
            else if (vertex.hasProperty(founded)){
              vertex.setFrom(vertex.getPropertyValue(founded).getLong());
              vertex.setTo(vertex.getPropertyValue(founded).getLong() + (1053108L *24L*14L));
            }
            else {
              vertex.setFrom(0L);
              vertex.setTo(Long.MAX_VALUE);
            }

            return vertex;
          }
        }, new TransformationFunction<Edge>() {
          public Edge apply(Edge edge, Edge el1) {

            String since = "since";
            String dateOfEntry = "dateOfEntry";

            if (edge.hasProperty(since)){
              edge.setFrom(edge.getPropertyValue(since).getLong());
              edge.setTo(edge.getPropertyValue(since).getLong() + (3612345L*24L*3L));
            }
            else if (edge.hasProperty(dateOfEntry)){
              edge.setFrom(edge.getPropertyValue(dateOfEntry).getLong());
              edge.setTo(edge.getPropertyValue(dateOfEntry).getLong() + (3654321L*24L*7L));
            }
            else {
              edge.setFrom(0L);
              edge.setTo(Long.MAX_VALUE);}
            return edge;
          }
        });
    return graph;
  }
}
