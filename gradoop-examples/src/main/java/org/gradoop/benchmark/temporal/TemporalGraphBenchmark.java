package org.gradoop.benchmark.temporal;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.gradoop.benchmark.subgraph.SubgraphBenchmark;
import org.gradoop.benchmark.temporal.SubgraphOperators.*;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.config.GradoopHBaseConfig;
import org.gradoop.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.storage.impl.hbase.factory.HBaseEPGMStoreFactory;
import org.gradoop.storage.impl.hbase.io.HBaseDataSource;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;


public class TemporalGraphBenchmark extends AbstractRunner implements ProgramDescription {
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
  private static final String OPTION_CSV_PATH = "c";
  /**
   * Use from timestamp
   */
  private static final String OPTION_FROM_TIMESTAMP = "f";
  /**
   * Use to timestamp
   */
  private static final String OPTION_TO_TIMESTAMP = "t";

  /**
   * Use querytype (asof, between, fromto, containedin, validduring, all, createdin, deletedin)
   */
  private static final String OPTION_QUERYTYPE = "q";

  /**
   * Use filter flag (hbase filter, subgraphs)
   */
  private static final String OPTION_FILTER = "r";

  /**
   * Use filter flag (hbase filter, subgraphs)
   */
  private static final String OPTION_PARALLELISM = "p";

  /**
   * Used input path
   */
  private static String INPUT_PATH;

  /**
   * Used output path
   */
  private static String OUTPUT_PATH;
  /**
   * Used from timestamp
   */
  private static String FROM_TS;
  /**
   * Used to timestamp
   */
  private static String TO_TS;
  /**
   * Used querytype
   */
  private static String QUERYTYPE;
  /**
   * Used csv path
   */
  private static String CSV_PATH;
  /**
   * Used parallelism
   */
  private static Integer PARALLELISM;
  /**
   * Used filter option
   */
  private static boolean FILTER;

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "prefix", true,
        "Prefix of source.");
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "output", true,
        "Path to output file");
    OPTIONS.addOption(OPTION_CSV_PATH, "csv", true,
        "Path to csv statistics");
    OPTIONS.addOption(OPTION_FROM_TIMESTAMP, "from", true,
        "From timestamp.");
    OPTIONS.addOption(OPTION_TO_TIMESTAMP, "to", true,
        "To timestamp.");
    OPTIONS.addOption(OPTION_QUERYTYPE, "querytype", true,
        "Used query type.");
    OPTIONS.addOption(OPTION_PARALLELISM, "parallelism", true,
        "Used parallelism.");
    OPTIONS.addOption(OPTION_FILTER, "filter", false,
        "Used filter");
  }

  /**
   * Main program to run the benchmark. Arguments are the available options.
   *
   * @param args program arguments
   * @throws Exception in case of Error
   */
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, TemporalGraphBenchmark.class.getName());

    if (cmd == null) {
      System.exit(1);
    }

    // test if minimum arguments are set
    performSanityCheck(cmd);

    // read cmd arguments
    readCMDArguments(cmd);

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig conf = GradoopFlinkConfig.createConfig(env);

    // set parallelism
    env.setParallelism(PARALLELISM);

    // create graph store
    LogicalGraph logGraph;
    HBaseEPGMStore graphStore = HBaseEPGMStoreFactory
        .createOrOpenEPGMStore(HBaseConfiguration.create(), GradoopHBaseConfig.getDefaultConfig(),
            INPUT_PATH + ".");

      //read graph from HBase
    HBaseDataSource hBaseSource = new HBaseDataSource(graphStore, conf);

    // restrict whole temporal graph by different filter options
    if (FILTER) {
      hBaseSource.setTemps(Long.valueOf(FROM_TS), Long.valueOf(TO_TS), querytypeDecision());
    }

    logGraph = hBaseSource.getLogicalGraph();

    switch (QUERYTYPE) {
      case "asof":
        FilterFunction<Vertex> vfilterASOF = new FilterASOF<>(Long.valueOf(FROM_TS));
        FilterFunction<Edge> efilterASOF = new FilterASOF<>(Long.valueOf(FROM_TS));
        logGraph = logGraph.subgraph(vfilterASOF, efilterASOF);
        break;
      case "fromto":
        FilterFunction<Vertex> vfilterFROM = new FilterFROM<>(Long.valueOf(FROM_TS), Long.valueOf(TO_TS));
        FilterFunction<Edge> efilterFROM = new FilterFROM<>(Long.valueOf(FROM_TS), Long.valueOf(TO_TS));
        logGraph =  logGraph.subgraph(vfilterFROM, efilterFROM);
        break;
      case "between":
        FilterFunction<Vertex> vfilterBETWEEN = new FilterBETWEEN<>(Long.valueOf(FROM_TS), Long.valueOf(TO_TS));
        FilterFunction<Edge> efilterBETWEEN = new FilterBETWEEN<>(Long.valueOf(FROM_TS), Long.valueOf(TO_TS));
        logGraph =  logGraph.subgraph(vfilterBETWEEN, efilterBETWEEN);
        break;
      case "containedin":
        FilterFunction<Vertex> vfilterCONTAINEDIN = new FilterCONTAINEDIN<>(Long.valueOf(FROM_TS), Long.valueOf(TO_TS));
        FilterFunction<Edge> efilterCONTAINEDIN = new FilterCONTAINEDIN<>(Long.valueOf(FROM_TS), Long.valueOf(TO_TS));
        logGraph =  logGraph.subgraph(vfilterCONTAINEDIN, efilterCONTAINEDIN);
        break;
      case "validduring":
        FilterFunction<Vertex> vfilterVALIDDURING = new FilterVALIDDURING<>(Long.valueOf(FROM_TS), Long.valueOf(TO_TS));
        FilterFunction<Edge> efilterVALIDDURING = new FilterVALIDDURING<>(Long.valueOf(FROM_TS), Long.valueOf(TO_TS));
        logGraph =  logGraph.subgraph(vfilterVALIDDURING, efilterVALIDDURING);
        break;
      case "createdin":
        FilterFunction<Vertex> vfilterCREATEDIN = new FilterCREATEDIN<>(Long.valueOf(FROM_TS), Long.valueOf(TO_TS));
        FilterFunction<Edge> efilterCREATEDIN = new FilterCREATEDIN<>(Long.valueOf(FROM_TS), Long.valueOf(TO_TS));
        logGraph =  logGraph.subgraph(vfilterCREATEDIN, efilterCREATEDIN);
        break;
      case "deletedin":
        FilterFunction<Vertex> vfilterDELETEDIN = new FilterDELETEDIN<>(Long.valueOf(FROM_TS), Long.valueOf(TO_TS));
        FilterFunction<Edge> efilterDELETEDIN = new FilterDELETEDIN<>(Long.valueOf(FROM_TS), Long.valueOf(TO_TS));
        logGraph =  logGraph.subgraph(vfilterDELETEDIN, efilterDELETEDIN);
        break;
      default:
    }

    //write graph to csv data sink
    DataSink sink = new CSVDataSink(OUTPUT_PATH, conf);
    sink.write(logGraph);


    // execute and write job statistics
    env.execute();
    writeCSV(env);
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
    if (!cmd.hasOption(OPTION_PARALLELISM)) {
      throw new IllegalArgumentException("Parallelism need to be set.");
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
    FROM_TS      = cmd.getOptionValue(OPTION_FROM_TIMESTAMP);
    TO_TS        = cmd.getOptionValue(OPTION_TO_TIMESTAMP);
    PARALLELISM  = Integer.parseInt(cmd.getOptionValue(OPTION_PARALLELISM));
    FILTER       = cmd.hasOption(OPTION_FILTER);
  }

  /**
   * reads and transforms the given querytype argument from command line into hbase querytype
   *
   */
  private static HBaseDataSource.Querytype querytypeDecision() {
    HBaseDataSource.Querytype chosenType;
    switch (QUERYTYPE) {
      case "asof":
        chosenType = HBaseDataSource.Querytype.AS_OF;
        break;
      case "between":
        chosenType = HBaseDataSource.Querytype.BETWEEN_IN;
        break;
      case "fromto":
        chosenType = HBaseDataSource.Querytype.FROM_TO;
        break;
      case "validduring":
        chosenType = HBaseDataSource.Querytype.VALID_DURING;
        break;
      case "containedin":
        chosenType = HBaseDataSource.Querytype.CONTAINED_IN;
        break;
      case "createdin":
        chosenType = HBaseDataSource.Querytype.CREATED_IN;
        break;
      case "deletedin":
        chosenType = HBaseDataSource.Querytype.DELETED_IN;
        break;
      default:
        chosenType = HBaseDataSource.Querytype.ALL;
        break;
    }
    return chosenType;
  }

  // generation of csv file with all relevant measurements
  private static void writeCSV(ExecutionEnvironment env) throws IOException {

    String head = String.format("%s|%s|%s|%s|%s|%s|%s%n",
        "SubsetFromHBase",
        "Querytype",
        "Dataset",
        "Parallelism",
        "From",
        "To",
        "Runtime(s)");

    String tail = String.format("%s|%s|%s|%s|%s|%s|%s%n",
        FILTER,
        QUERYTYPE,
        INPUT_PATH,
        PARALLELISM,
        FROM_TS,
        TO_TS,
        env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS));

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

  /**
   * {@inheritDoc}
   */
  @Override
  public String getDescription() { return TemporalGraphBenchmark.class.getName();}

}
