package com.yzong.ccproj4;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PopulateDB {

  static Configuration conf; // HBase database configuration.
  static BufferedReader buffer = new BufferedReader(new InputStreamReader(System.in));

  public static class Map extends Mapper<LongWritable, Text, ImmutableBytesWritable, Text> {

    /*-
     * Mapper: Given a phrase like "life is good--tab--100", the mapper would emit:
     *  ("life is good", "--tab--100")
     *  ("life is", "good--tab--100")
     * The mapper only emits key-value pairs for phrases that appear more than "threshold" times.
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
        InterruptedException {
      int threshold = context.getConfiguration().getInt("conf.PopulateDB.threshold", -1);
      String[] line = value.toString().split("\t");
      String newPhrase = line[0];
      String newCount = line[1];
      // We skip phrases that appear less than "threshold" times.
      if (Integer.parseInt(newCount) < threshold) {
        return;
      }
      int lastSpaceIdx = Math.max(newPhrase.lastIndexOf(" "), 0);
      String newBase = newPhrase.substring(0, lastSpaceIdx);
      String lastWord = newPhrase.substring(lastSpaceIdx + 1);
      context.write(new ImmutableBytesWritable(newPhrase.getBytes()), new Text("\t" + newCount));
      context.write(new ImmutableBytesWritable(newBase.getBytes()), new Text(lastWord + "\t"
          + newCount));
    }
  }

  /*-
   * Reducer:
   *   For any base phrase, we would receive the following pairs from mapper:
   *       ("life is", "--tab--1200")
   *       ("life is", "good--tab--100")
   *       ("life is", "wonderful--tab--200")
   *       ... ...
   *   For non-trivial entries, we sort them by frequency and populate HBase with top entries.
   */
  public static class Reduce
      extends TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable> {

    @Override
    public void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      int topnum = context.getConfiguration().getInt("conf.PopulateDB.topnum", -1);
      // HashMap maps a suggestion word to its frequency.
      HashMap<String, Long> wfMap = new HashMap<String, Long>();
      long total = 0; // Frequency of the base phrase.
      Iterator<Text> iValues = values.iterator();
      while (iValues.hasNext()) {
        String[] val = iValues.next().toString().split("\t");
        String word = val[0];
        long freq = Long.parseLong(val[1]);
        if (word.equals("")) {
          total = freq;
        } else {
          wfMap.put(word, freq);
        }
      }

      // Sanity check.
      if (total == 0) {
        System.err.printf("Malformed data for phrase %s!", key);
        System.exit(1);
      }
      // Ignore the case when no suggestions are available.
      if (wfMap.size() == 0) {
        return;
      }

      // Sort the HashMap by value in <i>descending</i> order.
      List<Entry<String, Long>> mapEntries = new ArrayList<Entry<String, Long>>(wfMap.entrySet());
      Collections.sort(mapEntries, new Comparator<Entry<String, Long>>() {
        public int compare(Entry<String, Long> o1, Entry<String, Long> o2) {
          return o2.getValue().compareTo(o1.getValue());
        }
      });

      // Write the results to HBase.
      Put put = new Put(key.copyBytes());
      for (int i = 1; i <= Math.min(topnum, mapEntries.size()); i++) {
        String wrd = mapEntries.get(i - 1).getKey();
        float prob = (float) mapEntries.get(i - 1).getValue() / total;
        put.add(("Suggestion" + i).getBytes(), "word".getBytes(), wrd.getBytes());
        put.add(("Suggestion" + i).getBytes(), "freq".getBytes(), Float.toString(prob).getBytes());
      }
      context.write(key, put);
    }
  }

  private static void initDatabase(Configuration conf) throws IOException {
    HBaseAdmin hadmin = new HBaseAdmin(conf);
    String corpusTable = conf.get("conf.PopulateDB.corpusTable");
    int topnum = conf.getInt("conf.PopulateDB.topnum", -1);
    while (hadmin.tableExists(corpusTable)) {
      System.out.printf("Table %s exists. All existing data will be deleted. Continue? (y/n)",
          corpusTable);
      int chr = (int) buffer.readLine().getBytes()[0];
      if (chr == 0x59 || chr == 0x79) {
        if (hadmin.isTableEnabled(corpusTable)) {
          hadmin.disableTable(corpusTable);
        }
        hadmin.deleteTable(corpusTable);
      } else if (chr == 0x4e || chr == 0x6e || chr < 0) {
        System.out.println("User Aborted!");
        System.exit(1);
      }
    }
    hadmin.createTable(new HTableDescriptor(corpusTable));
    // Add a column family for each suggestion word.
    hadmin.disableTable(corpusTable);
    for (int i = 1; i <= topnum; i++) {
      hadmin.addColumn(corpusTable, new HColumnDescriptor("Suggestion" + i));
    }
    hadmin.enableTable(corpusTable);
    hadmin.close();
  }

  // Parse command line options.
  private static CommandLine parseArgs(String[] args) {
    Options options = new Options();
    // Set up first argument.
    Option op = new Option("b", "table", true, "table to store statistics in");
    op.setArgName("tableName");
    op.setRequired(true);
    options.addOption(op);
    // Set up second argument.
    op = new Option("t", "threshold", true, "frequency threshold for a phrase to be ignored");
    op.setArgName("ignoreThreshold");
    op.setRequired(true);
    options.addOption(op);
    // Set up third argument.
    op = new Option("n", "suggestions", true, "number of suggestions given for a given phrase");
    op.setArgName("numOfSuggestions");
    op.setRequired(true);
    options.addOption(op);
    // Set up the parser.
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    }
    // Should errors happen, print out help message.
    catch (Exception e) {
      System.err.printf("ERROR: %s\n", e.getMessage());
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("populateDB", options, true);
      System.exit(1);
    }
    return cmd;
  }

  public static void main(String[] args) throws Exception {
    // Set up the HBase configuration and job.
    conf = HBaseConfiguration.create();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    CommandLine cmd = parseArgs(otherArgs);
    String corpusTable = cmd.getOptionValue("b");
    int threshold = Integer.parseInt(cmd.getOptionValue("t"));
    int topnum = Integer.parseInt(cmd.getOptionValue("n"));
    conf.set("conf.PopulateDB.corpusTable", corpusTable);
    conf.setInt("conf.PopulateDB.threshold", threshold);
    conf.setInt("conf.PopulateDB.topnum", topnum);
    System.out.println("Task properties:");
    System.out.println("\tTarget Table: " + corpusTable);
    System.out.println("\tFrequency threshold: " + threshold);
    System.out.println("\tNumber of Suggestions: " + topnum);

    initDatabase(conf);
    Job job = new Job(conf, "PopulateHBase");
    job.setJarByClass(PopulateDB.class);

    // Configure mapper-end details (HDFS => HBase).
    job.setMapperClass(Map.class);
    FileInputFormat.setInputPaths(job, "/mnt/input/");
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(Text.class);

    // Configure reducer-end details (HBase => HBase).
    job.setNumReduceTasks(16);
    TableMapReduceUtil.initTableReducerJob(corpusTable, Reduce.class, job);
    job.setOutputFormatClass(TableOutputFormat.class);
    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, corpusTable);

    // Run the job, displaying verbose information.
    job.waitForCompletion(true);
  }
}
