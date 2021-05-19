package com.magow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LatencyAverage {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Latency Count");
        job.setJarByClass(LatencyAverage.class);
        job.setMapperClass(LatencyAverage.LatencyMapper.class);
        job.setCombinerClass(LatencyAverage.AverageReducer.class);
        job.setReducerClass(LatencyAverage.AverageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class LatencyMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final String latencyRegex = "latency\\s*(\\d+)";
        private final String clientRegex = ".*client\\s*(\\d+)";
        private final Pattern latencyPattern = Pattern.compile(latencyRegex);
        private final Pattern clientPattern = Pattern.compile(clientRegex);
        private Text word = new Text();
        private IntWritable latency = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String line = itr.nextToken();
                Matcher latencyMatcher = latencyPattern.matcher(line);
                Matcher clientMatcher = clientPattern.matcher(line);
                if (clientMatcher.find() && latencyMatcher.find()) {
                    word.set("client" + clientMatcher.group(1));
                    latency.set(Integer.parseInt(latencyMatcher.group(1)));
                    context.write(word, latency);
                }
            }
        }
    }

    public static class AverageReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }
            result.set(sum/count);
            context.write(key, result);
        }
    }
}
