package com.enigma.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class WordFrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        // Sum all occurrences of the lemma for the (bookID, lemma, year) key
        for (IntWritable val : values) {
            sum += val.get();
        }

        // Remove the trailing comma if it exists
        String keyStr = key.toString();
        if (keyStr.endsWith(",")) {
            keyStr = keyStr.substring(0, keyStr.length() - 1);
        }
        
        context.write(new Text(keyStr), new IntWritable(sum));
    }
}