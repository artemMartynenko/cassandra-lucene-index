package com.stratio.cassandra.lucene.util;

/** Class for measuring time durations in milliseconds.
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public interface  TimeCounter {


    /** Returns the measured time in milliseconds.
     *
     * @return the measured time in milliseconds
     */
    long time();




    /** A started {@link TimeCounter}.*
     */
    class StartedTimeCounter implements TimeCounter{

        private final long startTime;
        private final long runTime;


        /**
         * @param startTime the start time in milliseconds
         * @param runTime   the already run time in milliseconds
         * */
        public StartedTimeCounter(long startTime, long runTime) {
            this.startTime = startTime;
            this.runTime = runTime;
        }


        @Override
        public long time() {
            return runTime + System.currentTimeMillis() - startTime;
        }

        /** Returns a new stopped time counter.
         *
         * @return a new stopped time counter
         */
        public StoppedTimeCounter stop(){
            return new StoppedTimeCounter(time());
        }

        @Override
        public String toString() {
            return time()+" ms";
        }
    }


    /** A stopped {@link TimeCounter}
     */
    class StoppedTimeCounter implements TimeCounter{


        private final long runTime;

        /**
         * @param runTime the total run time in milliseconds
         * */
        public StoppedTimeCounter(long runTime) {
            this.runTime = runTime;
        }


        @Override
        public long time() {
            return runTime;
        }


        /** Returns a new started time counter.
         *
         * @return a new started time counter
         */
        public StartedTimeCounter start(){
            return new StartedTimeCounter(System.currentTimeMillis(), time());
        }

        @Override
        public String toString() {
            return time()+" ms";
        }

    }




    /** Returns a new {@link StoppedTimeCounter}.
     *
     * @return a new stopped time counter
     */
    static StoppedTimeCounter create(){
        return new StoppedTimeCounter(0);
    }


    /** Returns a new {@link StartedTimeCounter}.
     *
     * @return a new started time counter
     */
    static StartedTimeCounter start(){
        return create().start();
    }



    /** Runs the specified closure and returns a stopped time counter measuring its execution time.
     *
     * @param f the closure to be run and measured
     * @return a new stopped time counter
     */
    static StoppedTimeCounter apply(Runnable f){
        StartedTimeCounter counter = create().start();
        f.run();
        return counter.stop();
    }
}
