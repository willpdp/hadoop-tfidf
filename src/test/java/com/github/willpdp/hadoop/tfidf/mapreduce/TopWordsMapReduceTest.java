package com.github.willpdp.hadoop.tfidf.mapreduce;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TopWordsMapReduceTest {

    @Mock
    public Mapper.Context mapperContext;

    @Mock
    public Reducer.Context reducerContext;

    @Test
    public void testMapper() throws IOException, InterruptedException {
        TopWordsMapReduce.TopWordsMapper topWordsMapper = new TopWordsMapReduce.TopWordsMapper();
        final Object key = new Object();
        topWordsMapper.map(key, new Text("a-midsummer-nights-dream.txt\tbottome:6.08"), mapperContext);
        verify(mapperContext, times(1)).write(eq(new Text("a-midsummer-nights-dream.txt")), eq(new Text("bottome:6.08")));
    }

    @Test
    public void testReducer() throws IOException, InterruptedException {
        TopWordsMapReduce.TopWordsReducer topWordsReducer = new TopWordsMapReduce.TopWordsReducer();
        final ImmutableList<Text> values = ImmutableList.of(new Text("foo:1"), new Text("bottome:6.08"));
        final Text book = new Text("a-midsummer-nights-dream.txt");
        topWordsReducer.reduce(book, values, reducerContext);
        ArgumentCaptor<Text> argumentCaptor = ArgumentCaptor.forClass(Text.class);
        verify(reducerContext, times(1)).write(eq(book), argumentCaptor.capture());
        assertThat(argumentCaptor.getValue().toString()).isEqualTo("bottome:6.08, foo:1.0");
    }

}