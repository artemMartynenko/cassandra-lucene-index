package com.stratio.cassandra.lucene.index;

import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;


/** [[Similarity]] that ignores the inverse document frequency, doing the similarity independent of the index
 * context.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 */
public class NoIDFSimilarity extends ClassicSimilarity {

    /** Returns a constant neutral score value of `1.0`.
     *
     * @param docFreq the number of documents which contain the term
     * @param numDocs the total number of documents in the collection
     * @return a constant value
     */
    @Override
    public float idf(long docFreq, long numDocs) {
        return 1.0f;
    }
}
