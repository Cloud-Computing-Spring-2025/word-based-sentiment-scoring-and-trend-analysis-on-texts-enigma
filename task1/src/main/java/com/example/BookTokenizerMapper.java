package com.example;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BookTokenizerMapper  extends Mapper<Object, Text, BookWritable, Text> {

    private static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
        "a", "an", "and", "are", "as", "at", "be", "but", "by",
        "for", "if", "in", "into", "is", "it", "of", "on", "or",
        "such", "that", "the", "their", "then", "there", "these",
        "they", "this", "to", "was", "will", "with", "you", "your",
        "i", "me", "my", "myself", "we", "our", "ours", "ourselves",
        "he", "him", "his", "himself", "she", "her", "hers", "herself",
        "itself", "who", "whom", "whose", "which", "while", "where",
        "when", "why", "how", "all", "any", "both", "each", "few",
        "more", "most", "other", "some", "such", "only", "own", 
        "same", "so", "than", "too", "very", "s", "t", "can", "just",
        "now", "not", "no", "nor", "again", "against", "between",
        "before", "after", "once", "here", "there", "because"
    ));

    private BookWritable bookDetails = new BookWritable();
    private Text word = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // the-title,the-year,the-author,the-text
        String[] split = value.toString().split(",", 4);
        if (split.length < 4) return;

        // Extract book details
        bookDetails.setTitle(split[0]);
        bookDetails.setYear(split[1]);
        bookDetails.setAuthor(split[2]);

        // Extract and Tokenize Text
        String line = split[3].replaceAll("[^a-zA-Z ]", "").toLowerCase();
        StringTokenizer tokens = new StringTokenizer(line);

        while (tokens.hasMoreTokens()) {
            String token = tokens.nextToken();
            if (STOP_WORDS.contains(token)) continue;

            word.set(token);
            context.write(bookDetails, word);
        }
    }
}
