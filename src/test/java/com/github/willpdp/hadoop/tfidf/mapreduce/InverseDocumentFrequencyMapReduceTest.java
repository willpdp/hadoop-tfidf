package com.github.willpdp.hadoop.tfidf.mapreduce;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

import static com.github.willpdp.hadoop.tfidf.TextFrequencyInverseDocumentFrequencyMapReduce.FILE_COUNT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class InverseDocumentFrequencyMapReduceTest {

    @Mock
    public Mapper.Context mapperContext;

    @Mock
    public Reducer.Context reducerContext;

    @Test
    public void testMapper() throws IOException, InterruptedException {
        InverseDocumentFrequencyMapReduce.InverseDocumentFrequencyMapper inverseDocumentFrequencyMapper = new InverseDocumentFrequencyMapReduce.InverseDocumentFrequencyMapper();
        final Object key = new Object();
        inverseDocumentFrequencyMapper.map(key, new Text("bottom@a-midsummer-nights-dream.txt 2"), mapperContext);
        verify(mapperContext, times(1)).write(eq(new Text("bottom")), eq(new Text("2@a-midsummer-nights-dream.txt")));
    }

    @Test
    public void testReducerRemovesCommonWords() throws IOException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set(FILE_COUNT, "3");
        when(reducerContext.getConfiguration()).thenReturn(configuration);
        InverseDocumentFrequencyMapReduce.InverseDocumentFrequencyReducer inverseDocumentFrequencyReducer = new InverseDocumentFrequencyMapReduce.InverseDocumentFrequencyReducer();
        final ImmutableList<Text> values = ImmutableList.of(new Text("1@a-midsummer-nights-dream.txt"),
                new Text("5@hamlet.txt"),
                new Text("2@macbeth.txt"));
        inverseDocumentFrequencyReducer.reduce(new Text("braines"), values, reducerContext);
        verify(reducerContext, times(0)).write(any(), any());
    }

    @Test
    public void testReducerKeepsUnusualWords() throws IOException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set(FILE_COUNT, "3");
        when(reducerContext.getConfiguration()).thenReturn(configuration);
        InverseDocumentFrequencyMapReduce.InverseDocumentFrequencyReducer inverseDocumentFrequencyReducer = new InverseDocumentFrequencyMapReduce.InverseDocumentFrequencyReducer();
        final ImmutableList<Text> values = ImmutableList.of(new Text("100@hamlet.txt"));
        inverseDocumentFrequencyReducer.reduce(new Text("hamlet"), values, reducerContext);
        verify(reducerContext, times(1)).write(eq(new Text("hamlet.txt")), eq(new Text("hamlet:109.86")));
    }

}