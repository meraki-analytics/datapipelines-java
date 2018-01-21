package com.merakianalytics.datapipelines.sinks;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.merakianalytics.datapipelines.PipelineContext;

/**
 * A {@link com.merakianalytics.datapipelines.sinks.DataSink} which delegates to other {@link com.merakianalytics.datapipelines.sinks.DataSink}s to fulfill
 * {@link com.merakianalytics.datapipelines.sinks.AbstractDataSink#put(Class, Object, PipelineContext)} and
 * {@link com.merakianalytics.datapipelines.sinks.AbstractDataSink#putMany(Class, Iterable, PipelineContext)} requests
 */
public class CompositeDataSink implements DataSink {
    private final Map<Class<?>, List<DataSink>> sinks;

    /**
     * @param sinks
     *        the {@link com.merakianalytics.datapipelines.sinks.DataSink}s to delegate to
     */
    public CompositeDataSink(final Collection<DataSink> sinks) {
        final Map<Class<?>, List<DataSink>> sinkMapping = new HashMap<>();
        for(final DataSink sink : sinks) {
            for(final Class<?> accepted : sink.accepts()) {
                List<DataSink> forType = sinkMapping.get(accepted);

                if(forType == null) {
                    forType = new LinkedList<>();
                    sinkMapping.put(accepted, forType);
                }

                forType.add(sink);
            }
        }

        for(final Class<?> type : sinkMapping.keySet()) {
            sinkMapping.put(type, Collections.unmodifiableList(sinkMapping.get(type)));
        }
        this.sinks = Collections.unmodifiableMap(sinkMapping);
    }

    @Override
    public Set<Class<?>> accepts() {
        return sinks.keySet();
    }

    @Override
    public <T> void put(final Class<T> type, final T item, final PipelineContext context) {
        final List<DataSink> sinks = this.sinks.get(type);

        if(sinks == null) {
            return;
        }

        for(final DataSink sink : sinks) {
            sink.put(type, item, context);
        }
    }

    @Override
    public <T> void putMany(final Class<T> type, final Iterable<T> items, final PipelineContext context) {
        final List<DataSink> sinks = this.sinks.get(type);

        if(sinks == null) {
            return;
        }

        for(final DataSink sink : sinks) {
            sink.putMany(type, items, context);
        }
    }
}
