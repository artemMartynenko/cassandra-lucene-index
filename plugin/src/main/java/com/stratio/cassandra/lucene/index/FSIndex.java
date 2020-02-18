package com.stratio.cassandra.lucene.index;

import org.apache.lucene.analysis.Analyzer;

import java.nio.file.Path;

/**
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class FSIndex {


    private String name;
    private Path path;
    private Analyzer analyzer;
    private Double refreshSeconds;
    private Integer ramBufferMb;
    private Integer maxMergeMb;
    private Integer maxCacheMb;


    public FSIndex(String name,
                   Path path,
                   Analyzer analyzer,
                   Double refreshSeconds,
                   Integer ramBufferMb,
                   Integer maxMergeMb,
                   Integer maxCacheMb) {
        this.name = name;
        this.path = path;
        this.analyzer = analyzer;
        this.refreshSeconds = refreshSeconds;
        this.ramBufferMb = ramBufferMb;
        this.maxMergeMb = maxMergeMb;
        this.maxCacheMb = maxCacheMb;
    }


}
