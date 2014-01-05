package com.nileshc.graphfu.pagerank;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author nilesh
 */
public abstract class AbstractMapReduceTest<MapperInputKey, MapperInputValue, MapperOutputKey, MapperOutputValue, ReducerOutputKey, ReducerOutputValue> {

    protected MapDriver<MapperInputKey, MapperInputValue, MapperOutputKey, MapperOutputValue> mapDriver;
    protected ReduceDriver<MapperOutputKey, MapperOutputValue, ReducerOutputKey, ReducerOutputValue> reduceDriver;
    protected MapReduceDriver<MapperInputKey, MapperInputValue, MapperOutputKey, MapperOutputValue, ReducerOutputKey, ReducerOutputValue> mapReduceDriver;
    protected Mapper<MapperInputKey, MapperInputValue, MapperOutputKey, MapperOutputValue> mapper;
    protected Reducer<MapperOutputKey, MapperOutputValue, ReducerOutputKey, ReducerOutputValue> reducer;
    protected Pair<MapperInputKey, MapperInputValue> mapperInput;
    protected Pair<MapperOutputKey, MapperOutputValue> mapperOutput;
    protected Pair<MapperOutputKey, List<MapperOutputValue>> reducerInput;
    protected Pair<ReducerOutputKey, ReducerOutputValue> reducerOutput;

    @Before
    public abstract void init() throws IOException;
    
    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(mapperInput).withOutput(mapperOutput).runTest();
    }

    @Test
    public void testReducer() throws IOException {
        reduceDriver.withInput(reducerInput).withOutput(reducerOutput).runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(mapperInput).withOutput(reducerOutput).runTest();
    }

    public abstract List<Pair<ReducerOutputKey, ReducerOutputValue>> getSampleOutput() throws IOException;
}