package com.github.willpdp.hadoop.tfidf.mapreduce;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TextFrequencyMapReduceTest {

    @Mock
    public Mapper.Context mapperContext;

    @Mock
    public Reducer.Context reducerContext;

    @Mock
    public FileSplit fileSplit;

    @Test
    public void testMapperStripsChars() throws IOException, InterruptedException {
        when(mapperContext.getInputSplit()).thenReturn(fileSplit);
        when(fileSplit.getPath()).thenReturn(new Path("hamlet.txt"));
        TextFrequencyMapReduce.TextFrequencyMapper textFrequencyMapper = new TextFrequencyMapReduce.TextFrequencyMapper();
        final Object key = new Object();
        textFrequencyMapper.map(key, new Text("Hello, this is a (String!)"), mapperContext);
        ArgumentCaptor<Text> argumentCaptor = ArgumentCaptor.forClass(Text.class);
        verify(mapperContext, times(5)).write(argumentCaptor.capture(), eq(new LongWritable(1)));
        assertThat(argumentCaptor.getAllValues().stream().map(x -> x.toString()).collect(Collectors.toList()))
                .containsExactlyElementsOf(ImmutableList.of("hello@hamlet.txt", "this@hamlet.txt", "is@hamlet.txt", "a@hamlet.txt", "string@hamlet.txt"));
    }

    @Test
    public void testReducer() throws IOException, InterruptedException {
        TextFrequencyMapReduce.TextFrequencyReducer textFrequencyReducer = new TextFrequencyMapReduce.TextFrequencyReducer();
        final ImmutableList<LongWritable> values = ImmutableList.of(new LongWritable(1), new LongWritable(1), new LongWritable(1), new LongWritable(1), new LongWritable(1));
        textFrequencyReducer.reduce(new Text("hello"), values, reducerContext);
        ArgumentCaptor<Text> argumentCaptor = ArgumentCaptor.forClass(Text.class);
        verify(reducerContext, times(1)).write(argumentCaptor.capture(), eq(new LongWritable(values.stream().mapToLong(x -> x.get()).sum())));
        assertThat(argumentCaptor.getAllValues().stream().map(x -> x.toString()).collect(Collectors.toList()))
                .containsExactlyElementsOf(ImmutableList.of("hello"));
    }


}