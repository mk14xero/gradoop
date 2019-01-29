package org.gradoop.benchmark.temporal;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class TemporalDataTest extends AbstractRunner implements ProgramDescription {

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = getExecutionEnvironment();
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

    DataSource source = new CSVDataSource(args[0], config);

    LogicalGraph graph = source.getLogicalGraph();

    graph.getVertices().filter(new FilterFunction<Vertex>() {
      @Override
      public boolean filter(Vertex vertex) throws Exception {
        return vertex.hasProperty("creationDate");
      }
    }).first(10).print();

  }

  @Override
  public String getDescription() {
    return TemporalDataTest.class.getName();
  }
}
