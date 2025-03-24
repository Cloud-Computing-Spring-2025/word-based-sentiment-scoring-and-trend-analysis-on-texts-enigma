# Multi Stage Sentiment Analysis with MapReduce and Hive

This project is a multi stage process to perform sentiment analysis with cloud computing tools.
The five stages are split between the group, and one member will handle each task.
The assignment is as follows:
1. Choose Dataset and Preprocess: **Hayden**
1. Word Frequency Analysis: **Shreyash**
1. Sentiment Scoring: **Usha**
1. Trend Analysis: **Adam**
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

Task 2 performs word frequency analysis with lemmatization on the preprocessed text. The MapReduce job tokenizes each sentence into individual words, applies lemmatization to convert words to their base forms, and then counts the frequency of each lemma per book and year.

The implementation uses a lightweight method for lemmatization without needing external NLP libraries. It relies on a mix of dictionary-based and rule-based techniques to convert words to their base forms.

## Key Features  

### Dictionary-Based Approach  
- It uses a built-in dictionary to handle words that don’t follow regular rules.  
- It covers exceptions like irregular plurals (e.g., **children → child**) and verb changes (e.g., **went → go**).  

### Rule-Based Processing  
- It follows basic rules to change words:  
  - **Plural to singular:** `cities → city`, `boxes → box`  
  - **Verbs to base form:** `walking → walk`, `baked → bake`  
  - **Adjectives to root form:** `faster → fast`, `fastest → fast`  

### Vowel Restoration  
- It fixes words where vowels change in different forms (e.g., **writing → write**).  

It makes lemmatization faster and more efficient without using heavy processing power.

### Execution Steps
1. cd into the `task2` directory
1. run `mvn install` to build the package
1. `docker cp target/word-frequency-analysis-1.0-SNAPSHOT.jar resourcemanager:/opt/hadoop-2.7.4/share/hadoop/mapreduce/` moves the jar into the container
1. `docker cp input resourcemanager:/word-frequency/input` copies input data into the container
1. Now, in the container, create input dir in hdfs: `hadoop fs -mkdir -p /input/dataset`
1. Copy input files from container to hdfs: `hadoop fs -put /word-frequency/input/*.txt /input/dataset`
1. Run the job:
`hadoop jar /opt/hadoop-2.7.4/share/hadoop/mapreduce/word-frequency-analysis-1.0-SNAPSHOT.jar com.enigma.controller.WordFrequencyController /input/dataset/from-india-to-planet-mars-parsed.txt /output`
1. For viewing results:
View results:
   ```bash
   hadoop fs -ls /user/root/
   hadoop fs -cat /user/root/com.enigma.controller.WordFrequencyController_20250318033529/part-r-00000
   ```
   Path:/user/root/com.enigma.controller.WordFrequencyController_20250318033529/

   Here Output path is created based on simple date format: yyyyMMddHHmmss
   
1. Copy results to local machine:
   ```bash
   hadoop fs -get /user/root/com.enigma.controller.WordFrequencyController_20250318040829/part-r-00000 /tmp/
   docker cp resourcemanager:/tmp/part-r-00000 ./FileName.txt
   ```

## Task 3: Sentiment Scoring
### 1. **Move to task3**
```bash
cd task3
```

### 2. **Start the Hadoop Cluster**

Run the following command to start the Hadoop cluster:

```bash
docker compose up -d
```

### 3. **Build the Code**

Build the code using Maven:

```bash
mvn clean package
```

### 4. **Copy JAR to Docker Container**

Copy the JAR file to the Hadoop ResourceManager container:

```bash
docker cp target/DocumentSimilarity-0.0.1-SNAPSHOT.jar resourcemanager:/opt/hadoop-2.7.4/share/hadoop/mapreduce/
```

### 5. **Move Dataset to Docker Container**

Copy the dataset to the Hadoop ResourceManager container:

```bash
 docker cp task2 resourcemanager:/opt/hadoop-2.7.4/share/hadoop/mapreduce/
```

### 6. **Connect to Docker Container**

Access the Hadoop ResourceManager container:

```bash
docker exec -it resourcemanager /bin/bash
```

Navigate to the Hadoop directory:

```bash
cd /opt/hadoop-2.7.4/share/hadoop/mapreduce/
```

### 7. **Set Up HDFS**

Create a folder in HDFS for the input dataset:

```bash
hadoop fs -mkdir -p /input/dataset
```

Copy the input dataset to the HDFS folder:

```bash
hadoop fs -put task2 /input/dataset
```

### 8. **Execute the MapReduce Job**

Run your MapReduce job using the following command:

```bash
hadoop jar /opt/hadoop-2.7.4/share/hadoop/mapreduce/DocumentSimilarity-0.0.1-SNAPSHOT.jar com.example.controller.SentimentAnalysisDriver /input/dataset/task2/output/the-art-of-natural-sleep-parsed.txt /output1
```

### 9. **View the Output**

To view the output of your MapReduce job, use:

```bash
hadoop fs -cat /output1/*
```

### 10. **Copy Output from HDFS to Local OS**

To copy the output from HDFS to your local machine:

1. Use the following command to copy from HDFS:
    ```bash
    hdfs dfs -get /output1 /opt/hadoop-2.7.4/share/hadoop/mapreduce/
    ```

2. use Docker to copy from the container to your local machine:
   ```bash
   exit 
   ```
    ```bash
    docker cp resourcemanager:/opt/hadoop-2.7.4/share/hadoop/mapreduce/output1/ ./output
    ```

## Task 4: Trend Analysis


## Task 5: Bigram Analysis

