//package edu.umd.cloud9.example.simple;
/*
 * Cloud9: A Hadoop toolkit for working with big data
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */



import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
public class PairsPMI_M extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI_M.class);
  private static long numRecords;
  
  protected static class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, FloatWritable> {
    private static final FloatWritable ONE = new FloatWritable(1);
    private static final PairOfStrings BIGRAM = new PairOfStrings();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString();
      
      HashSet<String> result = new HashSet<String>();
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
          result.add(itr.nextToken());
      }

       ArrayList<String> list = new ArrayList<String>(result);
    
       for (int x=0; x<list.size(); x++) {
         for(int y=x+1; y<list.size(); y++) {
           BIGRAM.set(list.get(x), list.get(y));
           context.write(BIGRAM, ONE);
         }
         BIGRAM.set(list.get(x), "*");
         context.write(BIGRAM, ONE);
       }
      }
   }

  protected static class MyCombiner extends
      Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {
    private static final FloatWritable SUM = new FloatWritable();

    @Override
    public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
      float sum = 0;
      Iterator<FloatWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  protected static class MyReducer extends
      Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {
    private static final FloatWritable VALUE = new FloatWritable();
    private float marginal = 0.0f;
    private static final PairOfStrings BIGRAM = new PairOfStrings();

    @Override
    public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
      float sum = 0.0f;
      Iterator<FloatWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }

      if(sum<10) return;
      if (key.getRightElement().equals("*")) {
        VALUE.set(sum);
        context.write(key, VALUE);
        marginal = sum;
      } else {
        if(sum>=10.0) {
          float tmp = ((float) sum) / ((float)marginal);
          VALUE.set(tmp);
          BIGRAM.set(key.getRightElement(), key.getLeftElement());
          context.write(BIGRAM, VALUE);
        }
      }
    }
  }


  protected static class MyPartitioner extends Partitioner<PairOfStrings, FloatWritable> {
    @Override
    public int getPartition(PairOfStrings key, FloatWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }
  
  
  protected static class MyMapperSecond extends Mapper<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {

    @Override
    public void map(PairOfStrings key, FloatWritable value, Context context)
        throws IOException, InterruptedException {
      context.write(key, value);
    }
  }

  protected static class MyReducerSecond extends Reducer<PairOfStrings, FloatWritable, Text, FloatWritable> {
private static final FloatWritable VALUE = new FloatWritable();
private float marginal = 0.0f;
private final static Text WORD = new Text();

@Override
public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
    throws IOException, InterruptedException {

  float val = values.iterator().next().get();

  WORD.set(key.getLeftElement()+" " + key.getRightElement());
//  long num = 156215;
  long num = context.getConfiguration().getLong("numRec", 1);
  
//  context.write(WORD, VALUE);

  if (key.getRightElement().equals("*")) {
    marginal = val/num;
  //  VALUE.set(marginal);
 //   context.write(WORD, VALUE);
  } else {
    
//    long tmp = (long) ( 1.0f*val/marginal);
////     float r = (float) Math.log10(tmp);
 //   VALUE.set((float) Math.log10(val/marginal));
    VALUE.set((float) Math.log10(val/marginal));

    context.write(WORD, VALUE);
    
  }
  
}
}
  
  
  private PairsPMI_M() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "numReducers";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of reducers").create(NUM_REDUCERS));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    // First MapReduce Job
    
    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ?
        Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool name: " + PairsPMI_M.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - tmp path: " + outputPath + "/tmp");
    LOG.info(" - num reducers: " + reduceTasks);

    Job job = Job.getInstance(getConf());
    job.setJobName(PairsPMI_M.class.getSimpleName());
    job.setJarByClass(PairsPMI_M.class);

    // Delete the tmp directory if it exists already
    Path tmpDir = new Path("tmp_wj");
    FileSystem.get(getConf()).delete(tmpDir, true);
    
    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path("tmp_wj"));

    job.setMapOutputKeyClass(PairOfStrings.class);
    job.setMapOutputValueClass(FloatWritable.class);
    job.setOutputKeyClass(PairOfStrings.class);
    job.setOutputValueClass(FloatWritable.class);
    
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
//    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyCombiner.class);
    job.setReducerClass(MyReducer.class);
    job.setPartitionerClass(MyPartitioner.class);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    double time1 = (System.currentTimeMillis() - startTime) / 1000.0;
    System.out.println("Job Finished in " + time1 + " seconds");
    numRecords = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS").getValue();
    
    /*
     *  Second MapReduce Job
     */
    
    LOG.info("Tool name: " + PairsPMI_M.class.getSimpleName());
    LOG.info("second stage of MapReduce");
    LOG.info(" - input from tmp path: " + outputPath + "/tmp_wj");
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - num reducers: " + reduceTasks);
    
    // set the global variable
    Configuration conf = getConf();
    conf.setLong("numRec", numRecords);
    
    job = Job.getInstance(getConf());
    job.setJobName(PairsPMI_M.class.getSimpleName());
    job.setJarByClass(PairsPMI_M.class);
    
    // Delete the output directory if it exists already
    Path outputDir = new Path(outputPath);
    FileSystem.get(getConf()).delete(outputDir, true);

    job.setNumReduceTasks(reduceTasks);
    FileInputFormat.setInputPaths(job, new Path("tmp_wj/part*"));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    
    job.setMapOutputKeyClass(PairOfStrings.class);
    job.setMapOutputValueClass(FloatWritable.class);
   // job.setOutputKeyClass(PairOfStrings.class);
    job.setOutputKeyClass(Text.class);

    job.setOutputValueClass(FloatWritable.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    //   job.setOutputFormatClass(SequenceFileOutputFormat.class);
       job.setOutputFormatClass(TextOutputFormat.class);

       job.setMapperClass(MyMapperSecond.class);
   //    job.setCombinerClass(MyCombiner.class);
       job.setReducerClass(MyReducerSecond.class);
       job.setPartitionerClass(MyPartitioner.class);
    
    startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    double time2 = (System.currentTimeMillis() - startTime)/1000.0;
    System.out.println("Second job finished in " + time2 + " seconds");
    System.out.println("Total time: " + (time1 + time2) + " seconds");
    
    
    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PairsPMI_M(), args);
  }
}
