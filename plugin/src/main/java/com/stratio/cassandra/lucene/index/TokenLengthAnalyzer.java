package com.stratio.cassandra.lucene.index;

import com.google.common.base.MoreObjects;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.FilteringTokenFilter;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
   * @author Andres de la Pena `adelapena@stratio.com`
   * @author Artem Martynenko artem7mag@gmail.com
   */
public class TokenLengthAnalyzer extends AnalyzerWrapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(TokenLengthAnalyzer.class);

    private final Analyzer analyzer;

    /**
     * @param analyzer the analyzer to be wrapped
     * */
    public TokenLengthAnalyzer(Analyzer analyzer) {
        super(analyzer.getReuseStrategy());
        this.analyzer = analyzer;
    }


    @Override
    protected Analyzer getWrappedAnalyzer(String s) {
        return analyzer;
    }


    @Override
    protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
        FilteringTokenFilter tokenFilter = new FilteringTokenFilter(components.getTokenStream()) {
            CharTermAttribute term = addAttribute(CharTermAttribute.class);
            int maxSize = IndexWriter.MAX_TERM_LENGTH;

            @Override
            protected boolean accept() throws IOException {
                int size = term.length();
                if(size <= maxSize){
                    return true;
                }else {
                    LOGGER.warn("Discarding immense term in field= {} ," +
                            " Lucene only allows terms with at most {} bytes in length; got {}"
                            , fieldName
                            , maxSize
                            , size);
                    return false;
                }
            }
        };

        return new TokenStreamComponents(components.getTokenizer(), tokenFilter);
    }

    public Analyzer analyzer() {
        return analyzer;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("analyzer", analyzer).toString();
    }
}
