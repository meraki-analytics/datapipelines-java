package com.merakianalytics.datapipelines;

import java.util.List;

import com.merakianalytics.datapipelines.transformers.DataTransformer;

/**
 * A series of transformations using {@link com.merakianalytics.datapipelines.transformers.DataTransformer}s to convert between types
 *
 * @see com.merakianalytics.datapipelines.transformers.DataTransformer
 */
@SuppressWarnings("rawtypes") // Class<?> specifier won't match the transform method signature. Have to pass it raw.
public class ChainTransform<F, T> {
    public static <S> ChainTransform<S, S> identity(final Class<S> type) {
        return new ChainTransform<>(type, type, null, null);
    }

    private final List<DataTransformer> chain;
    private final int cost;
    private final Class<F> from;
    private final Class<T> to;
    private final List<Class> typeSteps;

    /**
     * @param from
     *        the type this {@link com.merakianalytics.datapipelines.ChainTransform} converts from
     * @param to
     *        the type this {@link com.merakianalytics.datapipelines.ChainTransform} converts to
     * @param typeSteps
     *        the incremental types that the data will be converted to along the way
     * @param chain
     *        the {@link com.merakianalytics.datapipelines.transformers.DataTransformer} chain which accomplishes this conversion
     */
    public ChainTransform(final Class<F> from, final Class<T> to, final List<Class> typeSteps, final List<DataTransformer> chain) {
        this.from = from;
        this.to = to;
        this.typeSteps = typeSteps;
        this.chain = chain;

        int cost = 0;
        if(chain != null) {
            for(final DataTransformer transformer : chain) {
                cost += transformer.cost();
            }
        }
        this.cost = cost;
    }

    /**
     * @return the total transform cost
     */
    public int cost() {
        return cost;
    }

    /**
     * @return the type this {@link com.merakianalytics.datapipelines.ChainTransform} converts from
     */
    public Class<F> from() {
        return from;
    }

    /**
     * @return the type this {@link com.merakianalytics.datapipelines.ChainTransform} converts to
     */
    public Class<T> to() {
        return to;
    }

    /**
     * Converts data using this {@link com.merakianalytics.datapipelines.transformers.DataTransformer} chain
     *
     * @param item
     *        the data to convert
     * @param context
     *        information about the context of the request such as what {@link com.merakianalytics.datapipelines.DataPipeline} called this method
     * @return the converted data
     */
    @SuppressWarnings("unchecked") // T == F when the chain is empty
    public T transform(final F item, final PipelineContext context) {
        if(chain == null || chain.isEmpty()) {
            return (T)item;
        }

        Object current = item;
        for(int i = 0; i < chain.size(); i++) {
            current = chain.get(i).transform(typeSteps.get(i), typeSteps.get(i + 1), current, context);
        }
        return (T)typeSteps.get(typeSteps.size() - 1).cast(current);
    }
}
