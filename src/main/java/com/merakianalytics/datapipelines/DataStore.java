package com.merakianalytics.datapipelines;

import com.merakianalytics.datapipelines.sinks.DataSink;
import com.merakianalytics.datapipelines.sources.DataSource;

/**
 * A DataStore is the most common type of {@link com.merakianalytics.datapipelines.PipelineElement}. It is both a
 * {@link com.merakianalytics.datapipelines.sources.DataSource} and a {@link com.merakianalytics.datapipelines.sinks.DataSink}, as a database or cache might be.
 */
public interface DataStore extends DataSource, DataSink {}
