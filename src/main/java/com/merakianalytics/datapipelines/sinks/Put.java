package com.merakianalytics.datapipelines.sinks;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Designates a method as a delegate method for
 * {@link com.merakianalytics.datapipelines.sinks.AbstractDataSink#put(Class, Object, com.merakianalytics.datapipelines.PipelineContext)} calls
 *
 * @see com.merakianalytics.datapipelines.sinks.AbstractDataSink#put(Class, Object, com.merakianalytics.datapipelines.PipelineContext)
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Put {
    /**
     * @return the type which
     *         {@link com.merakianalytics.datapipelines.sinks.AbstractDataSink#put(Class, Object, com.merakianalytics.datapipelines.PipelineContext)} should
     *         delegate to this method for
     */
    public Class<?> value();
}
