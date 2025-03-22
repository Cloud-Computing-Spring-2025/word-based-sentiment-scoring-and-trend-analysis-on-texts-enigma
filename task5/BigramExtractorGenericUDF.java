package com.bigdata.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hive Generic UDF to extract bigrams from lemmatized text
 * Returns either a list of bigrams or their frequency counts
 */
public class BigramExtractorGenericUDF extends GenericUDF {
    private StringObjectInspector textOI;
    private StringObjectInspector modeOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // Validate arguments
        if (arguments.length != 1 && arguments.length != 2) {
            throw new UDFArgumentException("BigramExtractorGenericUDF requires 1 or 2 arguments");
        }
        
        // First argument must be a string (the text)
        if (!(arguments[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentException("First argument must be a string");
        }
        textOI = (StringObjectInspector) arguments[0];
        
        // Second argument (if provided) must be a string (the mode)
        if (arguments.length == 2) {
            if (!(arguments[1] instanceof StringObjectInspector)) {
                throw new UDFArgumentException("Second argument must be a string");
            }
            modeOI = (StringObjectInspector) arguments[1];
        }
        
        // Return an array of strings
        return ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        // Get text from the first argument
        if (arguments[0].get() == null) {
            return new ArrayList<>();
        }
        String text = textOI.getPrimitiveJavaObject(arguments[0].get());
        
        // Get mode from the second argument (if provided)
        String mode = "list";
        if (arguments.length == 2 && arguments[1].get() != null) {
            mode = modeOI.getPrimitiveJavaObject(arguments[1].get()).toLowerCase();
        }
        
        // Extract bigrams
        List<String> words = Arrays.asList(text.trim().split("\\s+"));
        List<String> bigrams = new ArrayList<>();
        
        for (int i = 0; i < words.size() - 1; i++) {
            String bigram = words.get(i) + " " + words.get(i + 1);
            bigrams.add(bigram);
        }
        
        // Return based on mode
        if (mode.equals("count")) {
            // Count frequencies
            Map<String, Integer> bigramCounts = new HashMap<>();
            for (String bigram : bigrams) {
                bigramCounts.put(bigram, bigramCounts.getOrDefault(bigram, 0) + 1);
            }
            
            // Convert to array representation
            List<String> result = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : bigramCounts.entrySet()) {
                result.add(entry.getKey() + ":" + entry.getValue());
            }
            
            return result;
        } else {
            // Return list of bigrams
            return bigrams;
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "BigramExtractorGenericUDF(" + String.join(", ", children) + ")";
    }
}