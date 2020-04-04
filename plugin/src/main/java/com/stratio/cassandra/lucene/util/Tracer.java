package com.stratio.cassandra.lucene.util;

import org.apache.cassandra.tracing.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**Wrapper for {@link Tracing} avoiding test environment failures.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class Tracer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Tracer.class);


    public boolean canTrace(){
        try {
           return Tracing.isTracing();
        } catch (Error e){
            LOGGER.warn(" Unable to trace: {}", e.getMessage());
            return false;
        }
    }




    /** Traces the specified string message.
     *
     * @param message the message to be traced
     */
    public void trace(String message){
        if(canTrace()){
            Tracing.trace(message);
        }
    }

}
