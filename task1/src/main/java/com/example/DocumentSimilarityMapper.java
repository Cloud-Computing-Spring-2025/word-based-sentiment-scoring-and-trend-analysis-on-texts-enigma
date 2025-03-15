package com.example;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DocumentSimilarityMapper  extends Mapper<Object, Text, Text, BookWritable> {

    private BookWritable document = new BookWritable();
    private Text word = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(": ");

        String line = split[1].replaceAll("[^a-zA-Z ]", "").toLowerCase();
        StringTokenizer tokens = new StringTokenizer(line);

        // Find unique words
        HashSet<String> words = new HashSet<>();
        while (tokens.hasMoreTokens())
            words.add(tokens.nextToken());
        
        document.setTitle(split[0]);
        document.setYear(words.size());

        for (String w : words) {
            word.set(w);
            context.write(word, document);
        }
    }
}
