package com.merakianalytics.datapipelines.sources;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Designates a method as a delegate method for
 * {@link com.merakianalytics.datapipelines.sources.AbstractDataSource#getMany(Class, java.util.Map, com.merakianalytics.datapipelines.PipelineContext)} calls
 *
 * @see com.merakianalytics.datapipelines.sources.AbstractDataSource#getMany(Class, java.util.Map, com.merakianalytics.datapipelines.PipelineContext)
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface GetMany {
    /**
     * @return the type which
     *         {@link com.merakianalytics.datapipelines.sources.AbstractDataSource#getMany(Class, java.util.Map, com.merakianalytics.datapipelines.PipelineContext)}
     *         should delegate to this method for
     */
    public Class<?> value();
}