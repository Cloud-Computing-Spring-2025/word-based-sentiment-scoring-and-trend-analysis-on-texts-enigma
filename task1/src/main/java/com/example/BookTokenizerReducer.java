package com.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BookTokenizerReducer extends Reducer<BookWritable, Text, BookWritable, Text> {

    private BookWritable document = new BookWritable();
    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void reduce(Text key, Iterable<BookWritable> values, Context context) throws IOException, InterruptedException {
        List<BookWritable> sorted = new ArrayList<>();
        for (BookWritable d: values) sorted.add(d);

        Collections.sort(sorted);

        // Make Pairs
        for (int i = 0; i < sorted.size() - 1; i++) {
            BookWritable a = sorted.get(i);

            for (int j = i + 1; j < sorted.size(); j++) {
                BookWritable b = sorted.get(j);

                document.setTitle(a.getTitle() + "_" + b.getTitle());
                document.setYear(a.getYear() + b.getYear());

                context.write(document, one);
            }
        }
    }
        
}
