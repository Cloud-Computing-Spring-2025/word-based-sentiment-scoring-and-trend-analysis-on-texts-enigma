import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;

public class TrendAnalysisMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final IntWritable sentimentScore = new IntWritable();
    private final Text outputKey = new Text();

}