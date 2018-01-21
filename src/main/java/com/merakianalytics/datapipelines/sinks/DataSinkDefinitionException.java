package com.merakianalytics.datapipelines.sinks;

/**
 * An exception for use by Annotation Processors on compilation, notifying developers when they've annotated a method that doesn't meet specification.
 *
 * @see com.merakianalytics.datapipelines.sinks.Put
 * @see com.merakianalytics.datapipelines.sinks.PutMany
 * @see com.merakianalytics.datapipelines.sinks.PutProcessor
 * @see com.merakianalytics.datapipelines.sinks.PutManyProcessor
 * @see com.merakianalytics.datapipelines.sinks.AbstractDataSink#put(Class, Object, com.merakianalytics.datapipelines.PipelineContext)
 * @see com.merakianalytics.datapipelines.sinks.AbstractDataSink#putMany(Class, Iterable, com.merakianalytics.datapipelines.PipelineContext)
 */
public class DataSinkDefinitionException extends RuntimeException {
    private static final long serialVersionUID = -727717811112509758L;

    /**
     * @param message
     *        the error message
     */
    public DataSinkDefinitionException(final String message) {
        super(message);
    }
}
