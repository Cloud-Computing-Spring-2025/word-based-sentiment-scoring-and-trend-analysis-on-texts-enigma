package com.example.controller;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.example.BookTokenizerMapper;
import com.example.BookTokenizerReducer;
import com.example.BookWritable;

public class BookTokenizer {
    
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = new Configuration();

        // Pair the documents to their words
        Job job = Job.getInstance(config, "Book Text Preprocessing");
        job.setJarByClass(BookTokenizer.class);

        job.setMapperClass(BookTokenizerMapper.class);
        job.setReducerClass(BookTokenizerReducer.class);

        job.setMapOutputKeyClass(BookWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(BookWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "/task1")); 

        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }

        System.exit(0);
    }
}