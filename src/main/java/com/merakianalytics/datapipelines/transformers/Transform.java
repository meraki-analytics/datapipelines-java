package com.merakianalytics.datapipelines.transformers;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Designates a method as a delegate method for
 * {@link com.merakianalytics.datapipelines.transformers.AbstractDataTransformer#transform(Class, Class, Object, com.merakianalytics.datapipelines.PipelineContext)}
 * calls
 *
 * @see com.merakianalytics.datapipelines.transformers.AbstractDataTransformer#transform(Class, Class, Object,
 *      com.merakianalytics.datapipelines.PipelineContext)
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Transform {
    /**
     * @return the type for which
     *         {@link com.merakianalytics.datapipelines.transformers.AbstractDataTransformer#transform(Class, Class, Object, com.merakianalytics.datapipelines.PipelineContext)}
     *         should delegate to this method when converting from
     */
    Class<?> from();

    /**
     * @return the type for which
     *         {@link com.merakianalytics.datapipelines.transformers.AbstractDataTransformer#transform(Class, Class, Object, com.merakianalytics.datapipelines.PipelineContext)}
     *         should delegate to this method when converting to
     */
    Class<?> to();
}
