package com.merakianalytics.datapipelines.transformers;

/**
 * An exception for use by Annotation Processors on compilation, notifying developers when they've annotated a method that doesn't meet specification.
 *
 * @see com.merakianalytics.datapipelines.transformers.Transform
 * @see com.merakianalytics.datapipelines.transformers.TransformProcessor
 * @see com.merakianalytics.datapipelines.transformers.AbstractDataTransformer#transform(Class, Class, Object,
 *      com.merakianalytics.datapipelines.PipelineContext)
 */
public class DataTransformerDefinitionException extends RuntimeException {
    private static final long serialVersionUID = 6674536573018565355L;

    /**
     * @param message
     *        the error message
     */
    public DataTransformerDefinitionException(final String message) {
        super(message);
    }
}
