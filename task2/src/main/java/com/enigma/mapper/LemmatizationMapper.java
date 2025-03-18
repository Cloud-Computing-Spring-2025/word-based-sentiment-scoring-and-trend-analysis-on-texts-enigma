package com.enigma.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang3.StringUtils;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Map;

public class LemmatizationMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Map<String, String> lemmaDict = new HashMap<>();

    @Override
    protected void setup(Context context) {
        // Initializing the common irregular words
        
        // Plural forms of words
        lemmaDict.put("children", "child");
        lemmaDict.put("men", "man");
        lemmaDict.put("women", "woman");
        lemmaDict.put("people", "person");
        lemmaDict.put("mice", "mouse");
        lemmaDict.put("feet", "foot");
        lemmaDict.put("teeth", "tooth");
        lemmaDict.put("geese", "goose");
        lemmaDict.put("data", "datum");
        lemmaDict.put("criteria", "criterion");
        lemmaDict.put("phenomena", "phenomenon");
        lemmaDict.put("alumni", "alumnus");
        lemmaDict.put("analyses", "analysis");
        lemmaDict.put("crises", "crisis");
        lemmaDict.put("theses", "thesis");
        
        // Common verb forms
        lemmaDict.put("am", "be");
        lemmaDict.put("is", "be");
        lemmaDict.put("are", "be");
        lemmaDict.put("was", "be");
        lemmaDict.put("were", "be");
        lemmaDict.put("been", "be");
        lemmaDict.put("being", "be");
        
        lemmaDict.put("has", "have");
        lemmaDict.put("had", "have");
        lemmaDict.put("having", "have");
        
        lemmaDict.put("does", "do");
        lemmaDict.put("did", "do");
        lemmaDict.put("done", "do");
        lemmaDict.put("doing", "do");
        
        lemmaDict.put("goes", "go");
        lemmaDict.put("went", "go");
        lemmaDict.put("gone", "go");
        lemmaDict.put("going", "go");
        
        lemmaDict.put("saw", "see");
        lemmaDict.put("seen", "see");
        
        lemmaDict.put("made", "make");
        lemmaDict.put("making", "make");
        
        lemmaDict.put("said", "say");
        lemmaDict.put("says", "say");
        
        lemmaDict.put("came", "come");
        lemmaDict.put("coming", "come");
        
        lemmaDict.put("took", "take");
        lemmaDict.put("taken", "take");
        
        // Some extra common forms
        lemmaDict.put("better", "good");
        lemmaDict.put("best", "good");
        lemmaDict.put("worse", "bad");
        lemmaDict.put("worst", "bad");
        lemmaDict.put("more", "many");
        lemmaDict.put("most", "many");
        lemmaDict.put("fewer", "few");
        lemmaDict.put("fewest", "few");
        lemmaDict.put("larger", "large");
        lemmaDict.put("largest", "large");
        lemmaDict.put("smaller", "small");
        lemmaDict.put("smallest", "small");
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        
        // Splitting line into parts using ":" for bookID and year, and  using "\t" for text
        String[] mainParts = line.split("\t", 2);
        if (mainParts.length < 2) {
            System.err.println("Invalid input line: " + line); // debugging invalid lines
            return;
        }

        String[] metadataParts = mainParts[0].split(":", 3); // Splitting by bookID and year
        if (metadataParts.length < 2) {
            System.err.println("Invalid metadata in line: " + line);
            return;
        }

        String bookID = metadataParts[0];
        String year = metadataParts[1];
        String text = mainParts[1].toLowerCase(); // operation to Lowercase the text

        // Tokenize text into words
        StringTokenizer tokenizer = new StringTokenizer(text);
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            token = token.replaceAll("[^a-zA-Z]", ""); // for Removing non-alphabetic characters
            
            if (StringUtils.isNotEmpty(token)) {
                String lemma = lemmatize(token);
                
                // Output as: (bookID, lemma, year) => 1
                word.set(bookID + "," + lemma + "," + year);
                context.write(word, one);
            }
        }
    }

    private String lemmatize(String token) {
        // Check if word is in our dictionary of irregular forms
        if (lemmaDict.containsKey(token)) {
            return lemmaDict.get(token);
        }
        
        // Implementing some basic suffix rules
        
        // For Handling plural nouns
        if (token.endsWith("ies") && token.length() > 3) {
            return token.substring(0, token.length() - 3) + "y";  // For example: cities -> city
        } else if (token.endsWith("ves") && token.length() > 3) {
            return token.substring(0, token.length() - 3) + "f";  // For example: wolves -> wolf
        } else if (token.endsWith("es") && token.length() > 2) {
            return token.substring(0, token.length() - 2);  // For example: boxes -> box
        } else if (token.endsWith("s") && token.length() > 1 && !token.endsWith("ss")) {
            return token.substring(0, token.length() - 1);  // For example: cats -> cat (but not (must be meaningful), miss -> mis)
        }
        
        // For Handling verb forms
        if (token.endsWith("ing") && token.length() > 4) {
            // Checking if we need to add 'e' back: writing -> write
            String stem = token.substring(0, token.length() - 3);
            if (needsEAfterStem(stem)) {
                return stem + "e";
            }
            return stem;  // For example: walking -> walk
        } else if (token.endsWith("ed") && token.length() > 3) {
            // For checking if we need to add 'e' back: baked -> bake
            String stem = token.substring(0, token.length() - 2);
            if (needsEAfterStem(stem)) {
                return stem + "e";
            }
            return stem;  // For example: walked -> walk
        }
        
        // For Handling adjective forms
        if (token.endsWith("er") && token.length() > 3) {
            return token.substring(0, token.length() - 2);  // For example: faster -> fast
        } else if (token.endsWith("est") && token.length() > 4) {
            return token.substring(0, token.length() - 3);  // For example: fastest -> fast
        }
        
        return token;
    }
    
    // For checking if we need to add 'e' after removing 'ing' or 'ed'
    private boolean needsEAfterStem(String stem) {
        // We have a simplified check here - we add 'e' if the stem ends with certain consonant patterns
        if (stem.length() < 2) return false;
        
        char lastChar = stem.charAt(stem.length() - 1);
        char secondLastChar = stem.charAt(stem.length() - 2);
        
        // Checking for patterns like 'ak' in 'bak-ed' -> 'bake'
        return isConsonant(lastChar) && isVowel(secondLastChar);
    }
    
    private boolean isConsonant(char c) {
        return "bcdfghjklmnpqrstvwxyz".indexOf(c) != -1;
    }
    
    private boolean isVowel(char c) {
        return "aeiou".indexOf(c) != -1;
    }
}