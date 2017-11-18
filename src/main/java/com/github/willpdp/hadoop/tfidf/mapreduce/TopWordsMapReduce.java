package com.github.willpdp.hadoop.tfidf.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

import static com.github.willpdp.hadoop.tfidf.TextFrequencyInverseDocumentFrequencyMapReduce.FILE_COUNT;

public class TopWordsMapReduce {

    public static class TopWordsMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            final StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
            context.write(new Text(stringTokenizer.nextToken()), new Text(stringTokenizer.nextToken()));
        }
    }

    public static class TopWordsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text book, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Double> words = new HashMap<>();
            for(Text word:values) {
                final StringTokenizer stringTokenizer = new StringTokenizer(word.toString(), ":");
                words.put(stringTokenizer.nextToken(), Double.parseDouble(stringTokenizer.nextToken()));
            }
            String output = String.join(", ", words.entrySet().stream()
                    .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                    .limit(30)
                    .map(entry -> entry.getKey()+":"+entry.getValue())
                    .collect(Collectors.toList()));
            context.write(book, new Text(output));
        }
    }

}
