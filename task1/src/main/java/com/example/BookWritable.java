package com.example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class BookWritable implements WritableComparable<BookWritable> {

    private String title;
    private String year;
    private String author;
    
    public BookWritable() {
        this.title = "no-data";
        this.year = "no-data";
        this.author = "no-data";
    }
    
    public BookWritable(String title, String year, String author) {
        this.title = title;
        this.year = year;
        this.author = author;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, title);
        WritableUtils.writeString(out, year);
        WritableUtils.writeString(out, author);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        title = WritableUtils.readString(in);
        year = WritableUtils.readString(in);
        author = WritableUtils.readString(in);
    }
    
    @Override
    public int compareTo(BookWritable o) {
        int cmp = title.compareTo(o.getTitle());
        if (cmp != 0) return cmp;
        return year.compareTo(o.getYear());
    }

    @Override
    public int hashCode() {
        return Objects.hash(title, year, author);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        BookWritable other = (BookWritable)obj;
        return Objects.equals(title, other.getTitle()) 
            && Objects.equals(year, other.getYear())
            && Objects.equals(author, other.getAuthor());
    }
    
    @Override
    public String toString() {
        return title + ":" + year + ":" + author; 
    }
    
    // Getters and Setters
    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getTitle() {
        return title;
    }
    
    public void setTitle(String id) {
        this.title = id;
    }
    
    public String getYear() {
        return year;
    }
    
    public void setYear(String year) {
        this.year = year;
    }
}
