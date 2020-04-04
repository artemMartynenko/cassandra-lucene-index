package com.stratio.cassandra.lucene;

import org.slf4j.helpers.MessageFormatter;

/**[[RuntimeException]] to be thrown when there are Lucene index-related errors.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class IndexException extends RuntimeException{

    /**
     * @param message the detail message
     * @param cause   the cause
     * */
    public IndexException(String message, Throwable cause) {
        super(message, cause);
    }

    /** Constructs a new index exception with the specified cause.
     *
     * @param cause the cause
     */
    public IndexException(Throwable cause) {
        super(cause);
    }

    /** Constructs a new index exception with the specified formatted detail message.
     *
     * @param message the detail message
     */
    public IndexException(String message) {
        super(message);
    }

    /** Constructs a new index exception with the specified formatted detail message.
     *
     * @param message the detail message
     * @param arg      first argument
     */
    public IndexException(String message, Object arg) {
        super(format1(message,arg));
    }


    /** Constructs a new index exception with the specified formatted detail message.
     *
     * @param message the detail message
     * @param arg1      first argument
     * @param arg2      second argument
     */
    public IndexException(String message, Object arg1, Object arg2){
        super(format2(message, arg1, arg2));
    }


    /** Constructs a new index exception with the specified formatted detail message.
     *
     * @param message the detail message
     * @param args      template args
     */
    public IndexException(String message, Object... args){
        super(formatN(message, args));
    }

    /** Constructs a new index exception with the specified formatted detail message.
     *
     * @param cause   the cause
     * @param message the detail message
     * @param arg      first argument
     */
    public IndexException(Throwable cause, String message, Object arg){
        super(format1(message, arg), cause);
    }


    /** Constructs a new index exception with the specified formatted detail message.
     *
     * @param cause   the cause
     * @param message the detail message
     * @param arg1      first argument
     * @param arg2      second argument
     */
    public IndexException(Throwable cause, String message, Object arg1, Object arg2){
        super(format2(message, arg1, arg2), cause);
    }

    /** Constructs a new index exception with the specified formatted detail message.
     *
     * @param cause   the cause
     * @param message the detail message
     * @param args      template args
     */
    public IndexException(Throwable cause, String message, Object... args){
        super(formatN(message, args), cause);
    }

    private static String format1(String message, Object arg){
        return MessageFormatter.format(message, arg).getMessage();
    }

    private static  String format2(String message, Object arg1, Object arg2){
        return MessageFormatter.format(message, arg1, arg2).getMessage();
    }

    private static String formatN(String message, Object... args){
        return MessageFormatter.arrayFormat(message, args).getMessage();
    }
}
