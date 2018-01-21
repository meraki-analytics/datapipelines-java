package com.merakianalytics.datapipelines;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.merakianalytics.datapipelines.iterators.CloseableIterator;
import com.merakianalytics.datapipelines.iterators.CloseableIterators;
import com.merakianalytics.datapipelines.sources.DataSource;

/**
 * Manages getting data out of a {@link com.merakianalytics.datapipelines.sources.DataSource} and providing it to the appropriate
 * {@link com.merakianalytics.datapipelines.sinks.DataSink}s.
 *
 * @see com.merakianalytics.datapipelines.sources.DataSource
 * @see com.merakianalytics.datapipelines.SinkHandler
 * @see com.merakianalytics.datapipelines.sinks.DataSink
 * @see com.merakianalytics.datapipelines.DataPipeline
 */
public class SourceHandler<P, A> {
    private class TransformingIterator implements CloseableIterator<P> {
        private final PipelineContext context;
        private final CloseableIterator<A> iterator;

        public TransformingIterator(final CloseableIterator<A> iterator, final PipelineContext context) {
            this.iterator = iterator;
            this.context = context;
        }

        @Override
        public void close() {
            iterator.close();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public P next() {
            final A item = iterator.next();

            for(final SinkHandler<A, ?> sink : preTransform) {
                sink.put(item, context);
            }

            final P transformed = transform.transform(item, context);

            for(final SinkHandler<P, ?> sink : postTransform) {
                sink.put(transformed, context);
            }

            return transformed;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(SourceHandler.class);
    private final Class<A> acquiredType;
    private final Set<SinkHandler<P, ?>> postTransform;
    private final Set<SinkHandler<A, ?>> preTransform;
    private final Class<P> providedType;
    private final DataSource source;
    private final ChainTransform<A, P> transform;

    /**
     * @param source
     *        the providing {@link com.merakianalytics.datapipelines.sources.DataSource}
     * @param transform
     *        the {@link com.merakianalytics.datapipelines.ChainTransform} which converts the data from the
     *        {@link com.merakianalytics.datapipelines.sources.DataSource} to the requested type
     * @param preTransform
     *        the {@link com.merakianalytics.datapipelines.SinkHandler}s which should be provided the data before the conversion
     * @param postTransform
     *        the {@link com.merakianalytics.datapipelines.SinkHandler}s which should be provided the data after the conversion
     * @param providedType
     *        the type this {@link com.merakianalytics.datapipelines.SourceHandler} provides
     * @param acquiredType
     *        the type the underlying {@link com.merakianalytics.datapipelines.sources.DataSource} provides
     */
    public SourceHandler(final DataSource source, final ChainTransform<A, P> transform, final Set<SinkHandler<A, ?>> preTransform,
        final Set<SinkHandler<P, ?>> postTransform, final Class<P> providedType, final Class<A> acquiredType) {
        this.source = source;
        this.transform = transform;
        this.preTransform = preTransform;
        this.postTransform = postTransform;
        this.providedType = providedType;
        this.acquiredType = acquiredType;
    }

    /**
     * @return the type the underlying {@link com.merakianalytics.datapipelines.sources.DataSource} provides
     */
    public Class<A> acquiredType() {
        return acquiredType;
    }

    /**
     * Gets data from the underlying {@link com.merakianalytics.datapipelines.sources.DataSource}, provides it to the appropriate
     * {@link com.merakianalytics.datapipelines.SinkHandler}s, converts it and returns it
     *
     * @param query
     *        a query specifying the details of what data should fulfill this request
     * @param context
     *        information about the context of the request such as what {@link com.merakianalytics.datapipelines.DataPipeline} called this method
     * @return the requested data
     */
    public P get(final Map<String, Object> query, final PipelineContext context) {
        final A item = source.get(acquiredType, query, context);

        if(item == null) {
            return null;
        }

        for(final SinkHandler<A, ?> sink : preTransform) {
            sink.put(item, context);
        }

        final P converted = transform.transform(item, context);

        for(final SinkHandler<P, ?> sink : postTransform) {
            sink.put(converted, context);
        }

        return converted;
    }

    /**
     * Gets multiple data elements from the underlying {@link com.merakianalytics.datapipelines.sources.DataSource}, provides them to the appropriate
     * {@link com.merakianalytics.datapipelines.SinkHandler}s, converts them and returns them
     *
     * @param query
     *        a query specifying the details of what data should fulfill this request
     * @param context
     *        information about the context of the request such as what {@link com.merakianalytics.datapipelines.DataPipeline} called this method
     * @return a {@link com.merakianalytics.datapipelines.iterators.CloseableIterator} of the request type if the query had a result, or null
     */
    public CloseableIterator<P> getMany(final Map<String, Object> query, final PipelineContext context) {
        return getMany(query, context, false);
    }

    /**
     * Gets multiple data elements from the underlying {@link com.merakianalytics.datapipelines.sources.DataSource}, provides them to the appropriate
     * {@link com.merakianalytics.datapipelines.SinkHandler}s, converts them and returns them
     *
     * @param query
     *        a query specifying the details of what data should fulfill this request
     * @param context
     *        information about the context of the request such as what {@link com.merakianalytics.datapipelines.DataPipeline} called this method
     * @param streaming
     *        whether to stream the results. If streaming is enabled, results from the {@link com.merakianalytics.datapipelines.sources.DataSource} will be
     *        converted and provided to the {@link com.merakianalytics.datapipelines.sinks.DataSink}s one-by-one as they are requested from the
     *        {@link com.merakianalytics.datapipelines.iterators.CloseableIterator} instead of converting and storing them all at once.
     * @return a {@link com.merakianalytics.datapipelines.iterators.CloseableIterator} of the request type if the query had a result, or null
     */
    public CloseableIterator<P> getMany(final Map<String, Object> query, final PipelineContext context, final boolean streaming) {
        if(!streaming) {
            final List<A> collector = new LinkedList<>();
            try(CloseableIterator<A> result = source.getMany(acquiredType, query, context)) {
                if(result == null) {
                    return null;
                }

                while(result.hasNext()) {
                    collector.add(result.next());
                }
            } catch(final Exception e) {
                LOGGER.error("Error closing results iterator!", e);
                throw new RuntimeException(e);
            }

            for(final SinkHandler<A, ?> sink : preTransform) {
                sink.putMany(collector, context);
            }

            final List<P> transformed = new LinkedList<>();
            for(final A item : collector) {
                transformed.add(transform.transform(item, context));
            }

            for(final SinkHandler<P, ?> sink : postTransform) {
                sink.putMany(transformed, context);
            }

            return CloseableIterators.from(transformed);
        } else {
            final CloseableIterator<A> result = source.getMany(acquiredType, query, context);
            if(result == null) {
                return null;
            }
            return new TransformingIterator(result, context);
        }
    }

    /**
     * @return the type this {@link com.merakianalytics.datapipelines.SourceHandler} provides
     */
    public Class<P> providedType() {
        return providedType;
    }
}
