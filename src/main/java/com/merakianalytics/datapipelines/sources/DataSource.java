package com.merakianalytics.datapipelines.sources;

import java.util.Map;
import java.util.Set;

import com.merakianalytics.datapipelines.PipelineContext;
import com.merakianalytics.datapipelines.PipelineElement;
import com.merakianalytics.datapipelines.iterators.CloseableIterator;

/**
 * A {@link com.merakianalytics.datapipelines.PipelineElement} that provides data within a {@link com.merakianalytics.datapipelines.DataPipeline}.
 *
 * A {@link com.merakianalytics.datapipelines.DataPipeline} is an ordered list of {@link com.merakianalytics.datapipelines.PipelineElement}s which data can be
 * requested from.
 *
 * The first {@link com.merakianalytics.datapipelines.sources.DataSource} in the pipeline which can provide data will be the one that provides it for a request.
 *
 * @see com.merakianalytics.datapipelines.DataPipeline
 * @see com.merakianalytics.datapipelines.PipelineElement
 */
public interface DataSource extends PipelineElement {
    /**
     * Gets some data from the {@link com.merakianalytics.datapipelines.sources.DataSource}. Will only be called for types that are supported by this
     * {@link com.merakianalytics.datapipelines.sources.DataSource}.
     *
     * @param <T>
     *        the type of the data that should be retrieved
     * @param type
     *        the type of the data that should be retrieved
     * @param query
     *        a query specifying the details of what data should fulfill this request
     * @param context
     *        information about the context of the request such as what {@link com.merakianalytics.datapipelines.DataPipeline} called this method
     * @return an object of the request type if the query had a result, or null
     */
    public <T> T get(final Class<T> type, final Map<String, Object> query, final PipelineContext context);

    /**
     * Gets multiple data elements from the {@link com.merakianalytics.datapipelines.sources.DataSource}. Will only be called for types that are supported by
     * this
     * {@link com.merakianalytics.datapipelines.sources.DataSource}.
     *
     * @param <T>
     *        the type of the data that should be retrieved
     * @param type
     *        the type of the data that should be retrieved
     * @param query
     *        a query specifying the details of what data should fulfill this request
     * @param context
     *        information about the context of the request such as what {@link com.merakianalytics.datapipelines.DataPipeline} called this method
     * @return a {@link com.merakianalytics.datapipelines.iterators.CloseableIterator} of the request type if the query had a result, or null
     */
    public <T> CloseableIterator<T> getMany(final Class<T> type, final Map<String, Object> query, final PipelineContext context);

    /**
     * @return the types that this {@link com.merakianalytics.datapipelines.sources.DataSource} provides
     */
    public Set<Class<?>> provides();
}
