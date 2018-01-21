package com.merakianalytics.datapipelines;

/**
 * Indicates that a certain type is not supported in the {@link com.merakianalytics.datapipelines.DataPipeline}
 */
public class NotSupportedException extends RuntimeException {
    private static final long serialVersionUID = -1727811546087506000L;

    /**
     * @param message
     *        the error message
     */
    public NotSupportedException(final String message) {
        super(message);
    }
}