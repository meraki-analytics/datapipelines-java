package com.merakianalytics.datapipelines.transformers;

import java.util.Map;
import java.util.Set;

import com.merakianalytics.datapipelines.PipelineContext;

/**
 * Supports the automatic conversion of types within a {@link com.merakianalytics.datapipelines.DataPipeline}
 *
 * @see com.merakianalytics.datapipelines.DataPipeline
 */
public interface DataTransformer {
    /**
     * @return a measure of how expensive this transformation operation is. The measure is purely relative to other transformers in the
     *         {@link com.merakianalytics.datapipelines.DataPipeline}.
     */
    public int cost();

    /**
     * Transforms data of one type to another. Will only be called for types that are supported by this
     * {@link com.merakianalytics.datapipelines.transformers.DataTransformer}.
     *
     * @param <F>
     *        the type to convert from
     * @param <T>
     *        the type to convert to
     * @param fromType
     *        the type to convert from
     * @param toType
     *        the type to convert to
     * @param item
     *        the data to convert
     * @param context
     *        information about the context of the request such as what {@link com.merakianalytics.datapipelines.DataPipeline} called this method
     * @return the converted data
     */
    public <F, T> T transform(final Class<F> fromType, final Class<T> toType, final F item, final PipelineContext context);

    /**
     * @return the conversions that this {@link com.merakianalytics.datapipelines.transformers.DataTransformer} supports
     */
    public Map<Class<?>, Set<Class<?>>> transforms();
}
