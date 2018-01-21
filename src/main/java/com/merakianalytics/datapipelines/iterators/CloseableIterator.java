package com.merakianalytics.datapipelines.iterators;

import java.util.Iterator;

/**
 * In the DataPipeline, iterators are used to allow streaming data from a source. Oftentimes database connections or similar will be involved which need to
 * clean up resources when the transaction is complete.
 */
public interface CloseableIterator<T> extends Iterator<T>, AutoCloseable {
    @Override
    public void close();
}
