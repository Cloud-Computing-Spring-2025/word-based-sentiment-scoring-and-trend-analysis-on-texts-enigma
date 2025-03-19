package com.example.controller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import com.example.TrendAnalysisMapper;
import com.example.TrendAnalysisReducer;

public class TrendAnalysisController {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TrendAnalysisController <input path> <output path>");
            System.exit(-1);
        }

        //create a new hadoop job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Trend Analysis");
        
        //set the Jar file
        job.setJarByClass(TrendAnalysisController.class);

        //set mapper and reducer classes
        job.setMapperClass(TrendAnalysisMapper.class);
        job.setReducerClass(TrendAnalysisReducer.class);

        //set the output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //set the input and output paths from arguments
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}