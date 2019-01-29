package org.gradoop.benchmark.temporal;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.config.GradoopHBaseConfig;
import org.gradoop.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.storage.impl.hbase.factory.HBaseEPGMStoreFactory;
import org.gradoop.storage.impl.hbase.io.HBaseDataSink;

public class WriteEPGMGraphToHBase {

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

            String creationDate = "creationDate";

            if (vertex.hasProperty(creationDate)){
              vertex.setProperty("from", vertex.getPropertyValue(creationDate).getLong());
              vertex.setProperty("to", vertex.getPropertyValue(creationDate).getLong() + (1053108L *24L*14L));
            }
            else {
              vertex.setProperty("from", 0L);
              vertex.setProperty("to", (Long.MAX_VALUE));
            }

            return vertex;
          }
        }, new TransformationFunction<Edge>() {
          public Edge apply(Edge edge, Edge el1) {

            String joinDate = "joinDate";
            String creationDate = "creationDate";

            if (edge.hasProperty(joinDate)){
              edge.setProperty("from", edge.getPropertyValue(joinDate).getLong());
              edge.setProperty("to", edge.getPropertyValue(joinDate).getLong() + (3612345L*24L*3L));
            }
            else if (edge.hasProperty(creationDate)){
              edge.setProperty("from", edge.getPropertyValue(creationDate).getLong());
              edge.setProperty("to", edge.getPropertyValue(creationDate).getLong() + (3654321L*24L*7L));
            }
            else {
              edge.setProperty("from", 0L);
              edge.setProperty("to", (Long.MAX_VALUE));}
            return edge;
          }
        });
    return graph;
  }
}
