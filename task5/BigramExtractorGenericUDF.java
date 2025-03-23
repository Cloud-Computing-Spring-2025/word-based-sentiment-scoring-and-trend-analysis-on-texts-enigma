package com.bigdata.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hive Generic UDF to extract bigrams from lemmatized text
 * Accepts a string of lemmatized text and returns a list of bigrams
 */
public class BigramExtractorGenericUDF extends GenericUDF {
    private StringObjectInspector wordOI;
    private IntObjectInspector countOI;
    
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // Validate arguments
        if (arguments.length != 2) {
            throw new UDFArgumentException("BigramExtractorGenericUDF requires exactly 2 arguments: word and count");
        }
        
        // First argument must be a string (the lemmatized word)
        if (!(arguments[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentException("First argument must be a string (word)");
        }
        
        // Second argument should be an integer (the count)
        if (!(arguments[1] instanceof IntObjectInspector)) {
            throw new UDFArgumentException("Second argument must be an integer (count)");
        }
        
        this.wordOI = (StringObjectInspector) arguments[0];
        this.countOI = (IntObjectInspector) arguments[1];
        
        // Return a list of strings (the bigrams)
        return ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0].get() == null || arguments[1].get() == null) {
            return null;
        }
        
        // Get the word and count
        String word = wordOI.getPrimitiveJavaObject(arguments[0].get());
        int count = countOI.get(arguments[1].get());
        
        // Split the word into tokens in case it contains multiple words
        String[] tokens = word.split("\\s+");
        List<String> bigrams = new ArrayList<>();
        
        // Generate bigrams
        for (int i = 0; i < tokens.length - 1; i++) {
            String bigram = tokens[i] + "_" + tokens[i + 1];
            // Add the bigram 'count' times to represent its frequency
            for (int j = 0; j < count; j++) {
                bigrams.add(bigram);
            }
        }
        
        return bigrams;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "extract_bigrams(" + String.join(", ", children) + ")";
    }
}

/*
----
-- Create the lemmatized_text table if it doesn't exist already
CREATE TABLE IF NOT EXISTS lemmatized_text (
    book_id STRING,
    word STRING,
    year INT,
    count STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Create a table to store bigram analysis results
CREATE TABLE bigram_frequencies (
    book_id STRING,
    bigram STRING,
    year INT,
    frequency INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

----
-- Load data from Task 2 output
LOAD DATA INPATH '/workspaces/word-based-sentiment-scoring-and-trend-analysis-on-texts-enigma/task2/output' 
INTO TABLE lemmatized_text;

-- Register the UDF JAR file
ADD JAR /opt/hive/BigramExtractorGenericUDF.jar;

-- Create temporary function
CREATE TEMPORARY FUNCTION extract_bigrams AS 'com.bigdata.hive.udf.BigramExtractorGenericUDF';

-------
-- Extract bigrams and calculate frequencies
INSERT OVERWRITE TABLE bigram_frequencies
SELECT 
    book_id,
    bigram,
    year,
    COUNT(*) as frequency
FROM (
    SELECT 
        book_id,
        explode(extract_bigrams(word, CAST(count AS INT))) as bigram,
        year
    FROM lemmatized_text
) bigram_data
GROUP BY book_id, bigram, year;

-- Query 1: Top 10 bigrams across all books
SELECT 
    bigram, 
    SUM(frequency) as total_frequency
FROM bigram_frequencies
GROUP BY bigram
ORDER BY total_frequency DESC
LIMIT 10;

-- Query 2: Top 5 bigrams for each book
SELECT * FROM (
    SELECT 
        book_id,
        bigram, 
        SUM(frequency) as total_frequency,
        ROW_NUMBER() OVER (PARTITION BY book_id ORDER BY SUM(frequency) DESC) as rank
    FROM bigram_frequencies
    GROUP BY book_id, bigram
) ranked
WHERE rank <= 5
ORDER BY book_id, rank;

-- Query 3: Bigram frequencies by decade
SELECT 
    CONCAT(CAST(FLOOR(year/10)*10 AS STRING), 's') as decade,
    bigram,
    SUM(frequency) as total_frequency
FROM bigram_frequencies
GROUP BY CONCAT(CAST(FLOOR(year/10)*10 AS STRING), 's'), bigram
HAVING SUM(frequency) > 10
ORDER BY decade, total_frequency DESC;

-----
-- Compare bigram usage changes over time
SELECT 
    b1.bigram,
    CONCAT(CAST(FLOOR(b1.year/10)*10 AS STRING), 's') as decade1,
    b1.freq as freq_decade1,
    CONCAT(CAST(FLOOR(b2.year/10)*10 AS STRING), 's') as decade2,
    b2.freq as freq_decade2,
    (b2.freq - b1.freq) as change
FROM 
    (SELECT bigram, FLOOR(year/10)*10 as year, SUM(frequency) as freq
     FROM bigram_frequencies
     GROUP BY bigram, FLOOR(year/10)*10) b1
JOIN 
    (SELECT bigram, FLOOR(year/10)*10 as year, SUM(frequency) as freq
     FROM bigram_frequencies
     GROUP BY bigram, FLOOR(year/10)*10) b2
ON b1.bigram = b2.bigram AND b1.year < b2.year
WHERE ABS(b2.freq - b1.freq) > 10
ORDER BY ABS(change) DESC
LIMIT 20;*/