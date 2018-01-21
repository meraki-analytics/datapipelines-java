package com.merakianalytics.datapipelines;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.merakianalytics.datapipelines.iterators.CloseableIterator;
import com.merakianalytics.datapipelines.sinks.DataSink;
import com.merakianalytics.datapipelines.sources.DataSource;
import com.merakianalytics.datapipelines.transformers.DataTransformer;

/**
 * A DataPipeline is an ordered list of {@link com.merakianalytics.datapipelines.PipelineElement}s:
 * {@link com.merakianalytics.datapipelines.sources.DataSource}s, {@link com.merakianalytics.datapipelines.sinks.DataSink}s, and
 * {@link com.merakianalytics.datapipelines.DataStore}s.
 *
 * It also contains {@link com.merakianalytics.datapipelines.transformers.DataTransformer}s, which convert the inputs and outputs of the
 * {@link com.merakianalytics.datapipelines.PipelineElement} to multiply the usefulness of the {@link com.merakianalytics.datapipelines.DataPipeline}.
 *
 * When data is requested from the {@link com.merakianalytics.datapipelines.DataPipeline}, it checks each
 * {@link com.merakianalytics.datapipelines.sources.DataSource} which provides the requested type in order, moving on to the next if null is provided.
 *
 * If a {@link com.merakianalytics.datapipelines.sources.DataSource} provides data, the data is provided to each
 * {@link com.merakianalytics.datapipelines.sinks.DataSink} that came before the providing {@link com.merakianalytics.datapipelines.sources.DataSource}, then
 * returned to the user.
 *
 * @see com.merakianalytics.datapipelines.sources.DataSource
 * @see com.merakianalytics.datapipelines.sinks.DataSink
 * @see com.merakianalytics.datapipelines.DataStore
 * @see com.merakianalytics.datapipelines.transformers.DataTransformer
 */
public class DataPipeline {
    private final Map<Class<?>, Object> sinkHandlerLocks;
    private final Map<Class<?>, Set<SinkHandler<?, ?>>> sinkHandlers;
    private final Set<DataSink> sinks;
    private final Map<Class<?>, Object> sourceHandlerLocks;
    private final Map<Class<?>, List<SourceHandler<?, ?>>> sourceHandlers;
    private final List<DataSource> sources;
    private final Map<DataSource, Set<DataSink>> sourceTargets;
    private final TypeGraph typeGraph;

    /**
     * @param transformers
     *        the {@link com.merakianalytics.datapipelines.transformers.DataTransformer}s to use in the {@link com.merakianalytics.datapipelines.DataPipeline}
     * @param elements
     *        the {@link com.merakianalytics.datapipelines.sources.DataSource}s, {@link com.merakianalytics.datapipelines.sinks.DataSink}s, and
     *        {@link com.merakianalytics.datapipelines.DataStore}s to use in the {@link com.merakianalytics.datapipelines.DataPipeline}
     */
    public DataPipeline(final Collection<? extends DataTransformer> transformers, final List<? extends PipelineElement> elements) {
        typeGraph = new TypeGraph(transformers);

        final Map<DataSource, Set<DataSink>> targets = new HashMap<>();
        final List<DataSource> sources = new LinkedList<>();
        final Set<DataSink> sinks = new HashSet<>();
        for(final PipelineElement element : elements) {
            if(element instanceof DataSource) {
                final DataSource source = (DataSource)element;
                sources.add(source);
                targets.put(source, Collections.unmodifiableSet(new HashSet<>(sinks)));
            }

            if(element instanceof DataSink) {
                sinks.add((DataSink)element);
            }
        }
        this.sources = Collections.unmodifiableList(sources);
        this.sinks = Collections.unmodifiableSet(sinks);
        sourceTargets = Collections.unmodifiableMap(targets);
        sourceHandlers = new ConcurrentHashMap<>();
        sinkHandlers = new ConcurrentHashMap<>();
        sourceHandlerLocks = new ConcurrentHashMap<>();
        sinkHandlerLocks = new ConcurrentHashMap<>();
    }

    /**
     * @param transformers
     *        the {@link com.merakianalytics.datapipelines.transformers.DataTransformer}s to use in the {@link com.merakianalytics.datapipelines.DataPipeline}
     * @param elements
     *        the {@link com.merakianalytics.datapipelines.sources.DataSource}s, {@link com.merakianalytics.datapipelines.sinks.DataSink}s, and
     *        {@link com.merakianalytics.datapipelines.DataStore}s to use in the {@link com.merakianalytics.datapipelines.DataPipeline}
     */
    public DataPipeline(final Collection<? extends DataTransformer> transformers, final PipelineElement... elements) {
        this(transformers, Arrays.asList(elements));
    }

    /**
     * @param elements
     *        the {@link com.merakianalytics.datapipelines.sources.DataSource}s, {@link com.merakianalytics.datapipelines.sinks.DataSink}s, and
     *        {@link com.merakianalytics.datapipelines.DataStore}s to use in the {@link com.merakianalytics.datapipelines.DataPipeline}
     */
    public DataPipeline(final PipelineElement... elements) {
        this(new LinkedList<DataTransformer>(), Arrays.asList(elements));
    }

    @SuppressWarnings({"unchecked", "rawtypes"}) // Can't get specific generic types for new SinkHandler
    private Entry<Set<SinkHandler<?, ?>>, Set<SinkHandler<?, ?>>> createSinkHandlers(final Class<?> before, final ChainTransform<?, ?> transform,
        final Class<?> after, final Set<DataSink> targets) {
        final Set<SinkHandler<?, ?>> beforeTransform = new HashSet<>();
        final Set<SinkHandler<?, ?>> afterTransform = new HashSet<>();
        for(final DataSink sink : targets) {
            final ChainTransform<?, ?> fromBefore = getBestTransform(before, sink.accepts());
            final ChainTransform<?, ?> fromAfter = getBestTransform(after, sink.accepts());

            if(fromBefore != null && fromAfter != null) {
                if(fromBefore.cost() < fromAfter.cost()) {
                    beforeTransform.add(new SinkHandler(sink, fromBefore, fromBefore.from(), fromBefore.to()));
                } else {
                    afterTransform.add(new SinkHandler(sink, fromAfter, fromAfter.from(), fromAfter.to()));
                }
            } else if(fromBefore != null) {
                beforeTransform.add(new SinkHandler(sink, fromBefore, fromBefore.from(), fromBefore.to()));
            } else if(fromAfter != null) {
                afterTransform.add(new SinkHandler(sink, fromAfter, fromAfter.from(), fromAfter.to()));
            }
        }
        return new SimpleImmutableEntry<>(beforeTransform, afterTransform);
    }

    @SuppressWarnings({"unchecked", "rawtypes"}) // Can't get specific generic types for new SinkHandler
    private Set<SinkHandler<?, ?>> createSinkHandlers(final Class<?> type, final Set<DataSink> targets) {
        final Set<SinkHandler<?, ?>> handlers = new HashSet<>();
        for(final DataSink sink : targets) {
            final ChainTransform<?, ?> transform = getBestTransform(type, sink.accepts());

            if(transform != null) {
                handlers.add(new SinkHandler(sink, transform, transform.from(), transform.to()));
            }
        }
        return Collections.unmodifiableSet(handlers);
    }

    @SuppressWarnings({"unchecked", "rawtypes"}) // Can't get specific generic types for new SourceHandler
    private List<SourceHandler<?, ?>> createSourceHandlers(final Class<?> type) {
        final List<SourceHandler<?, ?>> handlers = new LinkedList<>();
        for(final DataSource source : sources) {
            if(source.provides().contains(type)) {
                final Set<SinkHandler<?, ?>> sinkHandlers = createSinkHandlers(type, sourceTargets.get(source));
                handlers.add(new SourceHandler(source, ChainTransform.identity(type), sinkHandlers, Collections.emptySet(), type, type));
            } else {
                final ChainTransform<?, ?> transform = getBestTransform(source.provides(), type);

                if(transform != null) {
                    final Entry<Set<SinkHandler<?, ?>>, Set<SinkHandler<?, ?>>> created =
                        createSinkHandlers(transform.from(), transform, transform.to(), sourceTargets.get(source));
                    final Set<SinkHandler<?, ?>> preHandlers = created.getKey();
                    final Set<SinkHandler<?, ?>> postHandlers = created.getValue();
                    handlers.add(new SourceHandler(source, transform, preHandlers, postHandlers, transform.to(), transform.from()));
                }
            }
        }
        return Collections.unmodifiableList(handlers);
    }

    /**
     * Gets some data from the {@link com.merakianalytics.datapipelines.DataPipeline}
     *
     * @param <T>
     *        the type of the data that should be retrieved
     * @param type
     *        the type of the data that should be retrieved
     * @param query
     *        a query specifying the details of what data should fulfill this request
     * @return an object of the request type if the query had a result, or null
     */
    @SuppressWarnings("unchecked") // Pipeline ensures the proper type will be returned from get
    public <T> T get(final Class<T> type, final Map<String, Object> query) {
        final List<SourceHandler<?, ?>> handlers = getSourceHandlers(type);

        if(handlers.isEmpty()) {
            return null;
        }

        final PipelineContext context = newContext();

        for(final SourceHandler<?, ?> handler : handlers) {
            final T result = (T)handler.get(query, context);
            if(result != null) {
                return result;
            }
        }
        return null;
    }

    private ChainTransform<?, ?> getBestTransform(final Class<?> source, final Set<Class<?>> options) {
        ChainTransform<?, ?> best = null;
        int bestCost = Integer.MAX_VALUE;
        for(final Class<?> option : options) {
            final ChainTransform<?, ?> transform = typeGraph.getTransform(source, option);
            if(transform != null && transform.cost() < bestCost) {
                best = transform;
                bestCost = transform.cost();
            }
        }
        return best;
    }

    private ChainTransform<?, ?> getBestTransform(final Set<Class<?>> options, final Class<?> target) {
        ChainTransform<?, ?> best = null;
        int bestCost = Integer.MAX_VALUE;
        for(final Class<?> option : options) {
            final ChainTransform<?, ?> transform = typeGraph.getTransform(option, target);
            if(transform != null && transform.cost() < bestCost) {
                best = transform;
                bestCost = transform.cost();
            }
        }
        return best;
    }

    /**
     * Gets multiple data elements from the {@link com.merakianalytics.datapipelines.DataPipeline}
     *
     * @param <T>
     *        the type of the data that should be retrieved
     * @param type
     *        the type of the data that should be retrieved
     * @param query
     *        a query specifying the details of what data should fulfill this request
     * @return a {@link com.merakianalytics.datapipelines.iterators.CloseableIterator} of the request type if the query had a result, or null
     */
    public <T> CloseableIterator<T> getMany(final Class<T> type, final Map<String, Object> query) {
        return getMany(type, query, false);
    }

    /**
     * Gets multiple data elements from the {@link com.merakianalytics.datapipelines.DataPipeline}
     *
     * @param <T>
     *        the type of the data that should be retrieved
     * @param type
     *        the type of the data that should be retrieved
     * @param query
     *        a query specifying the details of what data should fulfill this request
     * @param streaming
     *        whether to stream the results. If streaming is enabled, results from the {@link com.merakianalytics.datapipelines.sources.DataSource} will be
     *        converted and provided to the {@link com.merakianalytics.datapipelines.sinks.DataSink}s one-by-one as they are requested from the
     *        {@link com.merakianalytics.datapipelines.iterators.CloseableIterator} instead of converting and storing them all at once.
     * @return a {@link com.merakianalytics.datapipelines.iterators.CloseableIterator} of the request type if the query had a result, or null
     */
    @SuppressWarnings("unchecked") // Pipeline ensures the proper type will be returned from getMany
    public <T> CloseableIterator<T> getMany(final Class<T> type, final Map<String, Object> query, final boolean streaming) {
        final List<SourceHandler<?, ?>> handlers = getSourceHandlers(type);

        if(handlers.isEmpty()) {
            return null;
        }

        final PipelineContext context = newContext();

        for(final SourceHandler<?, ?> handler : handlers) {
            final CloseableIterator<T> result = (CloseableIterator<T>)handler.getMany(query, context, streaming);
            if(result != null) {
                return result;
            }
        }
        return null;
    }

    private Set<SinkHandler<?, ?>> getSinkHandlers(final Class<?> type) {
        Set<SinkHandler<?, ?>> handlers = sinkHandlers.get(type);

        if(handlers == null) {
            Object lock = sinkHandlerLocks.get(type);
            if(lock == null) {
                synchronized(sinkHandlerLocks) {
                    lock = sinkHandlerLocks.get(type);
                    if(lock == null) {
                        lock = new Object();
                        sinkHandlerLocks.put(type, lock);
                    }
                }
            }
            synchronized(lock) {
                handlers = sinkHandlers.get(type);
                if(handlers == null) {
                    handlers = createSinkHandlers(type, sinks);
                    sinkHandlers.put(type, handlers);
                }
            }
        }

        return handlers;
    }

    private List<SourceHandler<?, ?>> getSourceHandlers(final Class<?> type) {
        List<SourceHandler<?, ?>> handlers = sourceHandlers.get(type);

        if(handlers == null) {
            Object lock = sourceHandlerLocks.get(type);
            if(lock == null) {
                synchronized(sourceHandlerLocks) {
                    lock = sourceHandlerLocks.get(type);
                    if(lock == null) {
                        lock = new Object();
                        sourceHandlerLocks.put(type, lock);
                    }
                }
            }
            synchronized(lock) {
                handlers = sourceHandlers.get(type);
                if(handlers == null) {
                    handlers = createSourceHandlers(type);
                    sourceHandlers.put(type, handlers);
                }
            }
        }

        return handlers;
    }

    private PipelineContext newContext() {
        final PipelineContext context = new PipelineContext();
        context.setPipeline(this);
        return context;
    }

    /**
     * Puts some data into all accepting {@link com.merakianalytics.datapipelines.sinks.DataSink}s in the {@link com.merakianalytics.datapipelines.DataPipeline}
     *
     * @param <T>
     *        the type of the data that should be stored
     * @param type
     *        the type of the data that should be stored
     * @param item
     *        the data to be stored
     */
    @SuppressWarnings("unchecked") // Pipeline ensures the proper type will be accepted by put
    public <T> void put(final Class<T> type, final T item) {
        final Set<SinkHandler<?, ?>> handlers = getSinkHandlers(type);

        if(handlers.isEmpty()) {
            return;
        }

        final PipelineContext context = newContext();

        for(final SinkHandler<?, ?> handler : handlers) {
            ((SinkHandler<T, ?>)handler).put(item, context);
        }
    }

    /**
     * Puts multiple data elements into all accepting {@link com.merakianalytics.datapipelines.sinks.DataSink}s in the
     * {@link com.merakianalytics.datapipelines.DataPipeline}
     *
     * @param <T>
     *        the type of the data that should be stored
     * @param type
     *        the type of the data that should be stored
     * @param items
     *        the data to be stored
     */
    @SuppressWarnings("unchecked") // Pipeline ensures the proper type will be accepted by putMany
    public <T> void putMany(final Class<T> type, final Iterable<T> items) {
        final Set<SinkHandler<?, ?>> handlers = getSinkHandlers(type);

        if(handlers.isEmpty()) {
            return;
        }

        final PipelineContext context = newContext();

        for(final SinkHandler<?, ?> handler : handlers) {
            ((SinkHandler<T, ?>)handler).putMany(items, context);
        }
    }
}
