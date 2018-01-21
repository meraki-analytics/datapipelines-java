package com.merakianalytics.datapipelines.sources;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.merakianalytics.datapipelines.PipelineContext;
import com.merakianalytics.datapipelines.iterators.CloseableIterator;

/**
 * A {@link com.merakianalytics.datapipelines.sources.DataSource} which delegates to other {@link com.merakianalytics.datapipelines.sources.DataSource}s to
 * fulfill
 * {@link com.merakianalytics.datapipelines.sources.AbstractDataSource#get(Class, Map, PipelineContext)} and
 * {@link com.merakianalytics.datapipelines.sources.AbstractDataSource#getMany(Class, Map, PipelineContext)} requests
 */
public class CompositeDataSource implements DataSource {
    private final Map<Class<?>, List<DataSource>> sources;

    /**
     * @param sources
     *        the {@link com.merakianalytics.datapipelines.sources.DataSource}s to delegate to
     */
    public CompositeDataSource(final Collection<? extends DataSource> sources) {
        final Map<Class<?>, List<DataSource>> sourceMapping = new HashMap<>();
        for(final DataSource source : sources) {
            for(final Class<?> provided : source.provides()) {
                List<DataSource> forType = sourceMapping.get(provided);

                if(forType == null) {
                    forType = new LinkedList<>();
                    sourceMapping.put(provided, forType);
                }

                forType.add(source);
            }
        }

        for(final Class<?> type : sourceMapping.keySet()) {
            sourceMapping.put(type, Collections.unmodifiableList(sourceMapping.get(type)));
        }
        this.sources = Collections.unmodifiableMap(sourceMapping);
    }

    @Override
    public <T> T get(final Class<T> type, final Map<String, Object> query, final PipelineContext context) {
        final List<DataSource> sources = this.sources.get(type);

        if(sources == null) {
            return null;
        }

        for(final DataSource source : sources) {
            final T result = source.get(type, query, context);
            if(result != null) {
                return result;
            }
        }

        return null;
    }

    @Override
    public <T> CloseableIterator<T> getMany(final Class<T> type, final Map<String, Object> query, final PipelineContext context) {
        final List<DataSource> sources = this.sources.get(type);

        if(sources == null) {
            return null;
        }

        for(final DataSource source : sources) {
            final CloseableIterator<T> result = source.getMany(type, query, context);
            if(result != null) {
                return result;
            }
        }

        return null;
    }

    @Override
    public Set<Class<?>> provides() {
        return sources.keySet();
    }
}
