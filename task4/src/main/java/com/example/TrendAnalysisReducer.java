package com.example;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class TrendAnalysisReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int total = 0;
        int count = 0;

        for (IntWritable val : values) {
            total += val.get();
            count++;
        }

        int average = count == 0 ? 0 : total / count;
        context.write(key, new IntWritable(average));
    }
}