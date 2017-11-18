package com.github.willpdp.hadoop.tfidf.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.stream.StreamSupport;

public class TextFrequencyMapReduce {

    public static class TextFrequencyMapper extends Mapper<Object, Text, Text, LongWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            final String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            final String line = value.toString().toLowerCase().replaceAll("[:,;!?.\\(\\)@]", "");
            final StringTokenizer stringTokenizer = new StringTokenizer(line);
            while(stringTokenizer.hasMoreTokens()) {
                Text word = new Text(stringTokenizer.nextToken()+"@"+filename);
                context.write(word, new LongWritable(1));
            }
        }
    }

    public static class TextFrequencyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            LongWritable sum = new LongWritable(StreamSupport.stream(values.spliterator(), false)
                    .mapToLong(value -> value.get())
                    .sum());
            context.write(key, sum);
        }
    }

}
