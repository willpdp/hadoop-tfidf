package com.github.willpdp.hadoop.tfidf;

import com.github.willpdp.hadoop.tfidf.mapreduce.InverseDocumentFrequencyMapReduce;
import com.github.willpdp.hadoop.tfidf.mapreduce.TextFrequencyMapReduce;
import com.github.willpdp.hadoop.tfidf.mapreduce.TopWordsMapReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TextFrequencyInverseDocumentFrequencyMapReduce {

    public static final String FILE_COUNT = "com.github.willpdp.hadoop.tfidf.file_count";

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        if(args.length>0 && args[0].equals("local")) {
            configuration.set("fs.default.name", "file:///");
        }
        // FIXME: do this properly
        configuration.set(FILE_COUNT, "3");

        Job job = Job.getInstance(configuration, "text-frequency");
        job.setJarByClass(TextFrequencyMapReduce.class);
        job.setMapperClass(TextFrequencyMapReduce.TextFrequencyMapper.class);
        job.setReducerClass(TextFrequencyMapReduce.TextFrequencyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path("text-frequency/input"));
        FileOutputFormat.setOutputPath(job, new Path("text-frequency/output"));
        try {
            job.waitForCompletion(true);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        job = Job.getInstance(configuration, "inverse-document-frequency");
        job.setJarByClass(InverseDocumentFrequencyMapReduce.class);
        job.setMapperClass(InverseDocumentFrequencyMapReduce.InverseDocumentFrequencyMapper.class);
        job.setReducerClass(InverseDocumentFrequencyMapReduce.InverseDocumentFrequencyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("text-frequency/output"));
        FileOutputFormat.setOutputPath(job, new Path("inverse-document-frequency/output"));
        try {
            job.waitForCompletion(true);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        job = Job.getInstance(configuration, "top-words");
        job.setJarByClass(TopWordsMapReduce.class);
        job.setMapperClass(TopWordsMapReduce.TopWordsMapper.class);
        job.setReducerClass(TopWordsMapReduce.TopWordsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("inverse-document-frequency/output"));
        FileOutputFormat.setOutputPath(job, new Path("top-words/output"));
        try {
            job.waitForCompletion(true);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

}
