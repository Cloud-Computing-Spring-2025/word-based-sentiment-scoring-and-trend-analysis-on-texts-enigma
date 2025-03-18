package com.example;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;
import java.util.*;

public class SentimentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Map<String, Integer> sentimentLexicon = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {
        // Define a simple sentiment lexicon directly in the Mapper
        sentimentLexicon.put("happy", 1);
        sentimentLexicon.put("sad", -1);
        sentimentLexicon.put("joyful", 2);
        sentimentLexicon.put("angry", -2);
        sentimentLexicon.put("neutral", 0);
        // Add more words and sentiment scores as needed
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Expected input format: bookID, word, year, frequency
        String[] parts = value.toString().split(",");
        if (parts.length < 4) return;  // Skip if the line doesn't have enough parts

        String bookID = parts[0];
        String word = parts[1].toLowerCase();  // Convert to lowercase for case-insensitive matching
        String year = parts[2];
        int frequency;

        try {
            frequency = Integer.parseInt(parts[3]);
        } catch (NumberFormatException e) {
            return;  // Skip if frequency is invalid
        }

        // Check if the word exists in the sentiment lexicon
        if (sentimentLexicon.containsKey(word)) {
            int sentimentScore = sentimentLexicon.get(word) * frequency;  
            // Multiply by frequency of the word
            System.out.println("Emitting output for: " + bookID + ", " + year + " -> " + sentimentScore);

            context.write(new Text(bookID + "," + year), new IntWritable(sentimentScore));  // Emit (bookID, year) -> sentimentScore
        }
    }
}