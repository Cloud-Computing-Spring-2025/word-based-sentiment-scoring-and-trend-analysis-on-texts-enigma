import org.apache.hadoop.hive.ql.exec.UDF;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class BigramUDF extends UDF {
    public Map<String,Integer> evaluate(List<String> words) {
        Map<String,Integer> bigramCounts = new HashMap<>();
        if (words == null || words.size() < 2) return bigramCounts;
        
        for(int i=0; i<words.size()-1; i++) {
            String bigram = words.get(i) + " " + words.get(i+1);
            bigramCounts.put(bigram, bigramCounts.getOrDefault(bigram, 0) + 1);
        }
        return bigramCounts;
    }
}
