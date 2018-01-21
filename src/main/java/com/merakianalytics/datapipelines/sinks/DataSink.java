package com.merakianalytics.datapipelines.sinks;

import java.util.Set;

import com.merakianalytics.datapipelines.PipelineContext;
import com.merakianalytics.datapipelines.PipelineElement;

/**
 * A {@link com.merakianalytics.datapipelines.PipelineElement} that stores data within a {@link com.merakianalytics.datapipelines.DataPipeline}.
 *
 * A {@link com.merakianalytics.datapipelines.DataPipeline} is an ordered list of {@link com.merakianalytics.datapipelines.PipelineElement}s which data can be
 * requested from.
 *
 * All {@link com.merakianalytics.datapipelines.sinks.DataSink}s that come before the {@link com.merakianalytics.datapipelines.sources.DataSource} which
 * provides the data in the pipeline will be given the data to store if they support the data type.
 *
 * @see com.merakianalytics.datapipelines.DataPipeline
 * @see com.merakianalytics.datapipelines.PipelineElement
 */
public interface DataSink extends PipelineElement {
    /**
     * @return the types that this {@link com.merakianalytics.datapipelines.sinks.DataSink} accepts
     */
    public Set<Class<?>> accepts();

    /**
     * Puts some data into the {@link com.merakianalytics.datapipelines.sinks.DataSink}. Will only be called for types that are supported by this
     * {@link com.merakianalytics.datapipelines.sinks.DataSink}.
     *
     * @param <T>
     *        the type of the data that should be stored
     * @param type
     *        the type of the data that should be stored
     * @param item
     *        the data to be stored
     * @param context
     *        information about the context of the request such as what {@link com.merakianalytics.datapipelines.DataPipeline} called this method
     */
    public <T> void put(final Class<T> type, final T item, final PipelineContext context);

    /**
     * Puts multiple data elements into the {@link com.merakianalytics.datapipelines.sinks.DataSink}. Will only be called for types that are supported by this
     * {@link com.merakianalytics.datapipelines.sinks.DataSink}.
     *
     * @param <T>
     *        the type of the data that should be stored
     * @param type
     *        the type of the data that should be stored
     * @param items
     *        the data to be stored
     * @param context
     *        information about the context of the request such as what {@link com.merakianalytics.datapipelines.DataPipeline} called this method
     */
    public <T> void putMany(final Class<T> type, final Iterable<T> items, final PipelineContext context);
}
