package com.enigma.controller;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class WordFrequencyController {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Frequency Analysis with Lemmatization");
        
        job.setJarByClass(WordFrequencyController.class);
        job.setMapperClass(com.enigma.mapper.LemmatizationMapper.class);
        job.setCombinerClass(com.enigma.reducer.WordFrequencyReducer.class); 
        job.setReducerClass(com.enigma.reducer.WordFrequencyReducer.class);
        
        // Setting the output key/value types for Mapper and Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // Setting input and output path
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[1])); 
        
        // Create a unique output path based on timestamp
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        String timestamp = dateFormat.format(new Date());
        String outputDir = args[0] + "_" + timestamp;
        FileOutputFormat.setOutputPath(job, new Path(outputDir)); // Output path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
