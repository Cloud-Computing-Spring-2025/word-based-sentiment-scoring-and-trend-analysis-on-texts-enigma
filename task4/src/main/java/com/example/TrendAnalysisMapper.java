package com.example;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TrendAnalysisMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final IntWritable sentimentScore = new IntWritable();
    private final Text outputKey = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Splitting input line by commas
        String[] fields = value.toString().split(",");
        
        // Skip malformed lines
        if (fields.length < 3) {
            return; 
        }
        try {
            // Extract book ID
            String bookID = fields[0].trim(); 

            // Extract year
            int year = Integer.parseInt(fields[fields.length - 2].trim());

            // Extract sentiment score or word frequency
            int score = Integer.parseInt(fields[fields.length - 1].trim());

            // Determine the decade
            int decade = (year / 10) * 10;
            String decadeStr = decade + "s";

            // Set score value
            sentimentScore.set(score);

            // Emit for overall trend
            outputKey.set(decadeStr);
            context.write(outputKey, sentimentScore);

            // Emit for book-level trend
            outputKey.set(bookID + "," + decadeStr);
            context.write(outputKey, sentimentScore);

        } catch (NumberFormatException e) {  
            // Ignore invalid numerical data
        }
    }
}