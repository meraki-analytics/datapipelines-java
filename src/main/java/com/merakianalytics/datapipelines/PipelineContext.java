package com.merakianalytics.datapipelines;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A container for contextual information about an active {@link com.merakianalytics.datapipelines.DataPipeline} request.
 * It allows information like remaining cache durations to be passed among {@link com.merakianalytics.datapipelines.PipelineElement}s
 */
public class PipelineContext extends ConcurrentHashMap<String, Object> {
    private static final long serialVersionUID = -4510900737770535444L;
    private DataPipeline pipeline;

    /**
     * @return the pipeline
     */
    public DataPipeline getPipeline() {
        return pipeline;
    }

    @Override
    public Object put(final String key, final Object value) {
        if(value == null) {
            return remove(key);
        } else {
            return super.put(key, value);
        }
    }

    /**
     * @param pipeline
     *        the pipeline to set
     */
    public void setPipeline(final DataPipeline pipeline) {
        this.pipeline = pipeline;
    }
}
