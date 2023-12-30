package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
public class JobSearchableContentNormalizationMap implements MapFunction<Job, Job> {

    @Override
    public Job map(Job job) throws Exception {
        Analyzer analyzer = new EnglishAnalyzer();
        StringBuilder normalizedString = new StringBuilder();

        try {
            // Tokenize and normalize the input string
            org.apache.lucene.analysis.TokenStream tokenStream = analyzer.tokenStream(null, job.getSearchableContent());
            tokenStream.reset();
            CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);

            while (tokenStream.incrementToken()) {
                // Append normalized tokens to the StringBuilder
                normalizedString.append(charTermAttribute.toString()).append(" ");
            }
            tokenStream.close();
            System.out.println("searchable content before normalization: " + job.getSearchableContent());
            System.out.println("searchable content after normalization: " + normalizedString.toString().trim());
            analyzer.close();
        } catch (Exception e) {
            // Handle exceptions
            e.printStackTrace();
        }
        job.setSearchableContent(normalizedString.toString().trim());
        return job;
    }
}
