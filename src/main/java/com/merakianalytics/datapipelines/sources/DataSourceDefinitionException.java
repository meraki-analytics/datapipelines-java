package com.merakianalytics.datapipelines.sources;

/**
 * An exception for use by Annotation Processors on compilation, notifying developers when they've annotated a method that doesn't meet specification.
 *
 * @see com.merakianalytics.datapipelines.sources.Get
 * @see com.merakianalytics.datapipelines.sources.GetMany
 * @see com.merakianalytics.datapipelines.sources.GetProcessor
 * @see com.merakianalytics.datapipelines.sources.GetManyProcessor
 * @see com.merakianalytics.datapipelines.sources.AbstractDataSource#get(Class, java.util.Map, com.merakianalytics.datapipelines.PipelineContext)
 * @see com.merakianalytics.datapipelines.sources.AbstractDataSource#getMany(Class, java.util.Map, com.merakianalytics.datapipelines.PipelineContext)
 */
public class DataSourceDefinitionException extends RuntimeException {
    private static final long serialVersionUID = 3574062294286233728L;

    /**
     * @param message
     *        the error message
     */
    public DataSourceDefinitionException(final String message) {
        super(message);
    }
}
