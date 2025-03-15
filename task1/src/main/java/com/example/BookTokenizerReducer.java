package com.example;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BookTokenizerReducer extends Reducer<BookWritable, Text, BookWritable, Text> {

    private Text fullText = new Text();

    @Override
    protected void reduce(BookWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<String> textLines = new ArrayList<String>();
        for (Text t: values) textLines.add(t.toString());

        fullText.set(String.join(" ", textLines));

        context.write(key, fullText);
    }
}
