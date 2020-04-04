package com.stratio.cassandra.lucene.util;

import com.stratio.cassandra.lucene.IndexException;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**  {@link TaskQueue} using parallel processing with thread pools.
 *
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class TaskQueueAsync implements TaskQueue {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskQueueAsync.class);
    private final int numThreads;
    private final int queuesSize;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private List<ThreadPoolExecutor> pools;


    /**
     * @param numThreads the number of executor threads
     * @param queuesSize the max number of tasks in each thread queue before blocking
     * */
    public TaskQueueAsync(int numThreads, int queuesSize) {
        this.numThreads = numThreads;
        this.queuesSize = queuesSize;
        pools = IntStream.rangeClosed(1, numThreads).boxed()
                .map(integer -> new ArrayBlockingQueue<Runnable>(queuesSize, true))
                .map(q -> new ThreadPoolExecutor(1, 1, 1, TimeUnit.DAYS, q,
                        new BasicThreadFactory.Builder().namingPattern("lucene-indexer-%d").build(),
                        getHandler()))
                .collect(Collectors.toList());
    }

    private RejectedExecutionHandler getHandler() {
        return (task, executor) -> {
            if (!executor.isShutdown()) {
                try {
                    executor.getQueue().put(task);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Override
    public void submitAsynchronous(Object id, Supplier<?> task) {
        lock.readLock().lock();
        try {
            pools.get(Math.abs(id.hashCode() % numThreads)).submit(task::get);
        } catch (Exception e) {
            LOGGER.error("Task queue asynchronous submission failed", e);
            throw new IndexException(e);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void submitSynchronous(Supplier<?> task) {
        lock.writeLock().lock();
        try {
            for (ThreadPoolExecutor executor : pools) {
                executor.submit(((() -> null))).get();// Wait for queued tasks completion
            }
        } catch (InterruptedException e) {
            LOGGER.error("Task queue await interrupted", e);
            throw new IndexException(e);
        } catch (ExecutionException e) {
            LOGGER.error("Task queue await failed", e);
            throw new IndexException(e);
        } catch (Exception e) {
            LOGGER.error("Task queue synchronous submission failed", e);
            throw new IndexException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            pools.forEach(ThreadPoolExecutor::shutdown);
        }finally {
            lock.writeLock().unlock();
        }
    }
}
