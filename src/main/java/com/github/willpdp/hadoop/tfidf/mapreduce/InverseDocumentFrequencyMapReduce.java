package com.github.willpdp.hadoop.tfidf.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import static com.github.willpdp.hadoop.tfidf.TextFrequencyInverseDocumentFrequencyMapReduce.FILE_COUNT;

public class InverseDocumentFrequencyMapReduce {

    public static class InverseDocumentFrequencyMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            final StringTokenizer lineTokenizer = new StringTokenizer(value.toString().toLowerCase());
            final StringTokenizer wordTokenizer = new StringTokenizer(lineTokenizer.nextToken(), "@");
            final String word = wordTokenizer.nextToken();
            final String book = wordTokenizer.nextToken();
            final String count = lineTokenizer.nextToken();
            context.write(new Text(word), new Text(count+"@"+book));
        }
    }

    public static class InverseDocumentFrequencyReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text word, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            final long fileCount = context.getConfiguration().getLong(FILE_COUNT, 1);
            final Map<String, Long> booksAndCount = new HashMap<>();
            for(Text value:values) {
                final StringTokenizer wordTokenizer = new StringTokenizer(value.toString(), "@");
                final Long count = Long.decode(wordTokenizer.nextToken());
                final String book = wordTokenizer.nextToken();
                booksAndCount.put(book, count);
            }
            final double idf = Math.log(((double)fileCount)/((double)booksAndCount.keySet().size()));
            if(booksAndCount.keySet().size() != fileCount) {
                for(String book:booksAndCount.keySet()) {
                    context.write(new Text(book), new Text(word.toString()+":"+String.format("%.2f",idf*booksAndCount.get(book))));
                }
            }
        }
    }

}
