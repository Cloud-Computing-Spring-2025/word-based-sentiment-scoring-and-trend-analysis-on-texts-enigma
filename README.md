# Multi Stage Sentiment Analysis with MapReduce and Hive

This project is a multi stage process to perform sentiment analysis with cloud computing tools.
The five stages are split between the group, and one member will handle each task.
The assignment is as follows:
1. Choose Dataset and Preprocess: **Hayden**
1. Word Frequency Analysis: **Adam**
1. Sentiment Scoring: **Usha**
1. Trend Analysis: **Shreyash**
1. Bigram Analysis: **Vinay**

## Dataset
Task 1 involved choosing a dataset of book texts from [project gutenburg](www.gutenberg.org).
The texts chosen are as follows:
- **From India to the planet Mars** by Théodore Flournoy, 1854
- **Law Ruslters** by Wilbur Tuttle, 1921
- **Little Grandfather** by Sophie May, 1833
- **Memoirs of a Sleep-Walker** by Charles Brown, 1771
- **The Art of Natural Sleep** by Lyman Powell, 1866

## Task 1: Preprocessing
Task 1 involves the selection, tokenization and cleaning of the input corpus.
All text from the five books were copied and placed into text files.
These files were fed to a MapReduce job which cleans and tokenizes the text.
The output of the job is a new file with the composite key for the book, along with the entire processed text in lowercase, space separated.<br><br>
The result of this process is parsed corpus of text, with stop words and punctuation removed, paired together with key information about the book.
This data can then be used down the pipeline for further processing.

### Task 1 Processing Steps
1. cd into the `task1` directory
1. run `mvn install` to build the package
1. `docker cp target/DocumentSimilarity-0.0.1-SNAPSHOT.jar resourcemanager:/opt/hadoop-2.7.4/share/hadoop/mapreduce/DocumentSimilarity.jar` moves the jar into the container
1. `docker cp input resourcemanager:/book-tokenizer/input` copies input data into the container
1. Now, in the container, create input dir in hdfs: `hadoop fs -mkdir -p /input/dataset`
1. Copy input files from container to hdfs: `hadoop fs -put /book-tokenizer/input/*.txt /input/dataset`
1. Ensure the output directory is removed: `hadoop fs -rm -r /output`
1. Ensure the local output directory is removed: `rm -r /book-tokenizer/output`
1. Run the job: `hadoop jar /opt/hadoop-2.7.4/share/hadoop/mapreduce/DocumentSimilarity.jar com.example.controller.BookTokenizer /input/dataset/*.txt /output`
1. Extract output: `hdfs dfs -get /output /book-tokenizer/`
1. Download output: `docker cp resourcemanager:/book-tokenizer/output output/`


## Task 2: Word Frequency Analysis


## Task 3: Sentiment Scoring


## Task 4: Trend Analysis


## Task 5: Bigram Analysis

