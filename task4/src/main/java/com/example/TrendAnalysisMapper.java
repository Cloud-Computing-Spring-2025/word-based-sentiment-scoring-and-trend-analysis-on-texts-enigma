import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Text;

import java.io.IOException;
import java.util.StringTokenizer;

import javax.naming.Context;

import java.util.HashMap;

public class TrendAnalysisMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final IntWritable sentimentScore = new IntWritable();
    private final Text outputKey = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //splitting input line by commas
        String[] fields = value.toString().split(",");
        //skip malformed lines
        if (fields.length < 3) {
            return; 
        }
        try {
            //extract book id
            String bookID = fields[0].trim(); 

            //extract year
            int year = Integer.parseInt(fields[fields.length - 2].trim());
            //extract sentiment or word Frequency
            int score = Integer.parseInt(fields[fields.length - 1].trim());

            //determine the decade
            int decade = (year / 10) * 10;
            String decadeStr = decade + "s";

            //emit for overall trend
            outputKey.set(decadeStr);
            outputValue.set(score);
            context.write(outputKey, outputValue);

            //emit for book-level trend
            outputKey.set(bookID + "," + decadeStr);
            context.write(outputKey, outputValue);

        //ignore invalid numerical data
        } catch (NumberFormatException e) {  
        }
    }
}