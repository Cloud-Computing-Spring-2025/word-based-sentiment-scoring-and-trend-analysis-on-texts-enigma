package com.enigma.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class WordFrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        
        // Sum all occurrences of the lemma for the (bookID, lemma, year) key
        for (IntWritable val : values) {
            sum += val.get();
        }
        
        // Output: (bookID, lemma, year) => frequency
        result.set(sum);
        context.write(key, result);
    }
}
