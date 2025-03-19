package com.example;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SentimentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Log the raw input line for debugging
        System.out.println("Processing line: " + value.toString());

        // Normalize the input line by replacing multiple spaces/tabs with a single space
        String line = value.toString().replaceAll("\\s+", " ").trim();
        String[] fields = line.split(",");

        // Validate the input line
        if (fields.length < 3) {
            System.err.println("Invalid line: " + line); // Log invalid lines
            return; // Skip invalid lines
        }

        // Extract relevant fields (e.g., bookID, year, sentimentScore)
        String bookID = fields[0].trim();
        String year = fields[1].trim();
        String rawSentimentScore = fields[2].trim();

        // Log the raw sentiment score for debugging
        System.out.println("Raw sentiment score: " + rawSentimentScore);

        int sentimentScore;
        try {
            // Remove any remaining spaces or tabs from the sentiment score before parsing
            sentimentScore = Integer.parseInt(rawSentimentScore.replaceAll("\\s+", ""));
        } catch (NumberFormatException e) {
            System.err.println("Invalid sentiment score: " + rawSentimentScore); // Log invalid scores
            return; // Skip lines with invalid sentiment scores
        }

        // Emit the composite key (bookID, year) and sentiment score
        String compositeKey = bookID + "," + year;
        System.out.println("Emitting: " + compositeKey + " -> " + sentimentScore); // Log emitted key-value pairs
        context.write(new Text(compositeKey), new IntWritable(sentimentScore));
    }
}
