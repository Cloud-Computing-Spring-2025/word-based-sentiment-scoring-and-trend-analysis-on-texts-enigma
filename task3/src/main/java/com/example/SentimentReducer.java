package com.example;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.*;

public class SentimentReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalSentimentScore = 0;

        // Sum up all sentiment scores for the given (bookID, year)
        for (IntWritable value : values) {
            totalSentimentScore += value.get();
        }

        // Emit the total sentiment score for the (bookID, year)
        context.write(key, new IntWritable(totalSentimentScore));
    }
}