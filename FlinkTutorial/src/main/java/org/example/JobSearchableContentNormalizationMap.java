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
            // a token is the minimimal searchable keyword in our indexing generated dictionary search的keyword的最小单位
            //拿到一个渔网叫tokenStream去网鱼，stream data就是水流里的鱼
            org.apache.lucene.analysis.TokenStream tokenStream = analyzer.tokenStream(null, job.getSearchableContent());
            //assure we are processing from the first letter of this input string
            tokenStream.reset();

            //设定这个渔网,只网鲈鱼（只要datastream里面的character）然后网鱼完成
            //相当于stream已经经过blackbox被分成了一个一个的token了
            CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);

            while (tokenStream.incrementToken()) {
                // Append normalized tokens to the StringBuilder
                //在每一个token后面加一个空格放入StringBuilder里面
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
