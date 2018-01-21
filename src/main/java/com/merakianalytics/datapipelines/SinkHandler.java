package com.merakianalytics.datapipelines;

import java.util.Iterator;

import com.merakianalytics.datapipelines.sinks.DataSink;

/**
 * Manages the process of putting data of a specific type into a {@link com.merakianalytics.datapipelines.sinks.DataSink}, including all necessary
 * transformations.
 *
 * @see com.merakianalytics.datapipelines.sinks.DataSink
 * @see com.merakianalytics.datapipelines.DataPipeline
 */
public class SinkHandler<A, S> {
    private class TransformingIterable implements Iterable<S> {
        private final PipelineContext context;
        private final Iterable<A> iterable;

        public TransformingIterable(final Iterable<A> iterable, final PipelineContext context) {
            this.iterable = iterable;
            this.context = context;
        }

        @Override
        public Iterator<S> iterator() {
            return new TransformingIterator(iterable.iterator(), context);
        }
    }

    private class TransformingIterator implements Iterator<S> {
        private final PipelineContext context;
        private final Iterator<A> iterator;

        public TransformingIterator(final Iterator<A> iterator, final PipelineContext context) {
            this.iterator = iterator;
            this.context = context;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public S next() {
            return transform.transform(iterator.next(), context);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private final Class<A> acceptedType;
    private final DataSink sink;
    private final Class<S> storedType;
    private final ChainTransform<A, S> transform;

    /**
     * @param sink
     *        the accepting {@link com.merakianalytics.datapipelines.sinks.DataSink}
     * @param transform
     *        the {@link com.merakianalytics.datapipelines.ChainTransform} which converts the acceptedType to the storedType
     * @param acceptedType
     *        the type this {@link com.merakianalytics.datapipelines.SinkHandler} accepts
     * @param storedType
     *        the type the {@link com.merakianalytics.datapipelines.sinks.DataSink} accepts
     */
    public SinkHandler(final DataSink sink, final ChainTransform<A, S> transform, final Class<A> acceptedType, final Class<S> storedType) {
        this.sink = sink;
        this.transform = transform;
        this.acceptedType = acceptedType;
        this.storedType = storedType;
    }

    /**
     * @return the type the underlying {@link com.merakianalytics.datapipelines.sinks.DataSink} accepts
     */
    public Class<A> acceptedType() {
        return acceptedType;
    }

    /**
     * Converts some data and provides it to the underlying {@link com.merakianalytics.datapipelines.sinks.DataSink}
     *
     * @param item
     *        the data to provide to the underlying {@link com.merakianalytics.datapipelines.sinks.DataSink}
     * @param context
     *        information about the context of the request such as what {@link com.merakianalytics.datapipelines.DataPipeline} called this method
     */
    public void put(final A item, final PipelineContext context) {
        final S transformed = transform.transform(item, context);
        sink.put(storedType, transformed, context);
    }

    /**
     * Converts multiple data elements and provides them to the underlying {@link com.merakianalytics.datapipelines.sinks.DataSink}
     *
     * @param items
     *        the data to provide to the underlying {@link com.merakianalytics.datapipelines.sinks.DataSink}
     * @param context
     *        information about the context of the request such as what {@link com.merakianalytics.datapipelines.DataPipeline} called this method
     */
    public void putMany(final Iterable<A> items, final PipelineContext context) {
        final Iterable<S> transforming = new TransformingIterable(items, context);
        sink.putMany(storedType, transforming, context);
    }

    /**
     * @return the type this {@link com.merakianalytics.datapipelines.SinkHandler} accepts
     */
    public Class<S> storedType() {
        return storedType;
    }
}
