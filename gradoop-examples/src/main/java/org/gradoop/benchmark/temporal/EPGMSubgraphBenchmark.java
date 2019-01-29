package org.gradoop.benchmark.temporal;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.gradoop.benchmark.temporal.EPGMSubgraphOperators.*;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.config.GradoopHBaseConfig;
import org.gradoop.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.storage.impl.hbase.factory.HBaseEPGMStoreFactory;
import org.gradoop.storage.impl.hbase.io.HBaseDataSource;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;


public class EPGMSubgraphBenchmark extends AbstractRunner implements ProgramDescription {
  /**
   * Option to declare prefix
   */
  private static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to declare path to output graph
   */
  private static final String OPTION_OUTPUT_PATH = "o";
  /**
   * Option to declare output path to statistics csv file
   */
  private static final String OPTION_CSV_PATH = "j";

  /**
   * Use querytype (asof, between, fromto, containedin, validduring, all, createdin, deletedin)
   */
  private static final String OPTION_QUERYTYPE = "q";

  private static final String OPTION_FROM_TIMESTAMP = "f";
  private static final String OPTION_TO_TIMESTAMP = "t";

  /**
   * Used input path
   */
  private static String INPUT_PATH;

  /**
   * Used output path
   */
  private static String OUTPUT_PATH;
  /**
   * Used querytype
   */
  private static String QUERYTYPE;
  /**
   * Used csv path
   */
  private static String CSV_PATH;

  private static long FROM;
  private static long TO;

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "prefix", true,
        "Prefix of source.");
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "output", true,
        "Path to output file");
    OPTIONS.addOption(OPTION_CSV_PATH, "csv", true,
        "Path to csv statistics");
    OPTIONS.addOption(OPTION_QUERYTYPE, "querytype", true,
        "Used query type.");
    OPTIONS.addOption(OPTION_FROM_TIMESTAMP, "from", true,
        "From Timestamp");
    OPTIONS.addOption(OPTION_TO_TIMESTAMP, "to", true,
        "To Timestamp");
  }

  /**
   * Main program to run the benchmark. Arguments are the available options.
   *
   * @param args program arguments
   * @throws Exception in case of Error
   */
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, EPGMSubgraphBenchmark.class.getName());

    if (cmd == null) {
      System.exit(1);
    }

    // test if minimum arguments are set
    performSanityCheck(cmd);

    // read cmd arguments
    readCMDArguments(cmd);

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig conf = GradoopFlinkConfig.createConfig(env);

    // create graph store
    LogicalGraph logGraph;
    HBaseEPGMStore graphStore = HBaseEPGMStoreFactory
        .createOrOpenEPGMStore(HBaseConfiguration.create(), GradoopHBaseConfig.getDefaultConfig(),
            INPUT_PATH + ".");

    //read graph from HBase
    HBaseDataSource hBaseSource = new HBaseDataSource(graphStore, conf);

    logGraph = hBaseSource.getLogicalGraph();

    LogicalGraph filteredGraph = filter(logGraph, FROM, TO, QUERYTYPE);


//    //write graph to csv data sink
    DataSink sink = new CSVDataSink(OUTPUT_PATH, conf);
    sink.write(filteredGraph);

    // execute and write job statistics
    env.execute();

    int parallelism = env.getParallelism();
    long runtime = env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS);

    writeCSV(parallelism, runtime);
  }

  /**
   * Checks if the minimum of arguments is provided
   *
   * @param cmd command line
   */
  private static void performSanityCheck(CommandLine cmd) {
    if (!cmd.hasOption(OPTION_INPUT_PATH)) {
      throw new IllegalArgumentException("Define a prefix.");
    }
    if (!cmd.hasOption(OPTION_CSV_PATH)) {
      throw new IllegalArgumentException("Path to CSV-File need to be set.");
    }
    if (!cmd.hasOption(OPTION_OUTPUT_PATH)) {
      throw new IllegalArgumentException("Define a graph output directory.");
    }
    if (!cmd.hasOption(OPTION_FROM_TIMESTAMP)) {
      throw new IllegalArgumentException("Define a from timestamp.");
    }
    if (!cmd.hasOption(OPTION_TO_TIMESTAMP)) {
      throw new IllegalArgumentException("Define a to timestamp.");
    }
  }

  /**
   * Reads the given arguments from command line
   *
   * @param cmd command line
   */
  private static void readCMDArguments(CommandLine cmd) {
    INPUT_PATH   = cmd.getOptionValue(OPTION_INPUT_PATH);
    CSV_PATH     = cmd.getOptionValue(OPTION_CSV_PATH);
    OUTPUT_PATH  = cmd.getOptionValue(OPTION_OUTPUT_PATH);
    QUERYTYPE    = cmd.getOptionValue(OPTION_QUERYTYPE);
    FROM         = Long.valueOf(cmd.getOptionValue(OPTION_FROM_TIMESTAMP));
    TO           = Long.valueOf(cmd.getOptionValue(OPTION_TO_TIMESTAMP));
  }

  // generation of csv file with all relevant measurements
  private static void writeCSV(int parallelism, long runtime) throws IOException {

    String head = String.format("%s|%s|%s|%s|%s|%s%n",
        "Dataset",
        "Parallelism",
        "Querytype",
        "From",
        "To",
        "Runtime(s)");

    String tail = String.format("%s|%s|%s|%s|%s|%s%n",
        INPUT_PATH,
        parallelism,
        QUERYTYPE,
        FROM,
        TO,
        runtime);

    File f = new File(CSV_PATH);
    if (f.exists() && !f.isDirectory()) {
      FileUtils.writeStringToFile(f, tail, true);
    } else {
      PrintWriter writer = new PrintWriter(CSV_PATH, "UTF-8");
      writer.print(head);
      writer.print(tail);
      writer.close();
    }
  }

  private static LogicalGraph getEWMP(LogicalGraph input, String lbAttribute, String ubAttribute) {
    return input.subgraph(new FilterFunction<Vertex>() {

      /**
       * Serial
       */
      private static final long serialVersionUID = 1L;

      @Override
      public boolean filter(Vertex arg0) throws Exception {
        return ((!arg0.hasProperty(lbAttribute)) || (!arg0.hasProperty(ubAttribute)));
      }

    }, new FilterFunction<Edge>() {

      /**
       * Serial
       */
      private static final long serialVersionUID = 1L;

      @Override
      public boolean filter(Edge arg0) throws Exception {
        return ((!arg0.hasProperty(lbAttribute)) || (!arg0.hasProperty(ubAttribute)));
      }
    });
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static LogicalGraph filter(LogicalGraph input, Long from, Long to, String strategy) {
    LogicalGraph filteredGraph = input;

    switch (strategy) {
      case "asof":
        FilterFunction filterASOF = new FilterASOF<>("from", "to", from);
        filteredGraph = input.subgraph(filterASOF, filterASOF);
        break;
      case "from":
        FilterFunction filterFROM = new FilterFROM<>("from", "to", from, to);
        filteredGraph = input.subgraph(filterFROM, filterFROM);
        break;
      case "between":
        FilterFunction filterBETWEEN = new FilterBETWEEN<>("from", "to", from, to);
        filteredGraph = input.subgraph(filterBETWEEN, filterBETWEEN);
        break;
      case "containedin":
        FilterFunction filterCONTAINEDIN = new FilterCONTAINEDIN<>("from", "to", from, to);
        filteredGraph = input.subgraph(filterCONTAINEDIN, filterCONTAINEDIN);
        break;
      case "validduring":
        FilterFunction filterVALIDDURING = new FilterVALIDDURING<>("from", "to", from, to);
        filteredGraph = input.subgraph(filterVALIDDURING, filterVALIDDURING);
        break;
      case "createdin":
        FilterFunction filterCREATEDIN = new FilterCREATEDIN<>("from", "to", from, to);
        filteredGraph = input.subgraph(filterCREATEDIN, filterCREATEDIN);
        break;
      case "deletedin":
        FilterFunction filterDELETEDIN = new FilterDELETEDIN<>("from", "to", from, to);
        filteredGraph = input.subgraph(filterDELETEDIN, filterDELETEDIN);
        break;
    }


    return filteredGraph;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getDescription() { return EPGMSubgraphBenchmark.class.getName();}

}
