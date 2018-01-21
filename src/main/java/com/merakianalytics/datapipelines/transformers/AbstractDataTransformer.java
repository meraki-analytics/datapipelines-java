package com.merakianalytics.datapipelines.transformers;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.merakianalytics.datapipelines.PipelineContext;

/**
 * A base class for easily defining new {@link com.merakianalytics.datapipelines.transformers.DataTransformer}s using just the
 * {@link com.merakianalytics.datapipelines.transformers.Transform} annotations.
 *
 * Methods annotated with these annotations in subclasses will be automatically picked up as delegate methods for
 * {@link com.merakianalytics.datapipelines.transformers.AbstractDataTransformer#transform(Class, Class, Object, PipelineContext)}.
 *
 * The {@link com.merakianalytics.datapipelines.transformers.AbstractDataTransformer#transforms()} functionality will be automatically determined from the
 * annotations as well.
 *
 * @see com.merakianalytics.datapipelines.transformers.Transform
 */
public class AbstractDataTransformer implements DataTransformer {
    private static final int DEFAULT_COST = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDataTransformer.class);
    private final Object initLock = new Object();
    private Map<Class<?>, Map<Class<?>, Method>> transformMethods;

    @Override
    public int cost() {
        return DEFAULT_COST;
    }

    private void initialize() {
        synchronized(initLock) {
            if(transformMethods != null) {
                return;
            }

            final Class<? extends AbstractDataTransformer> clazz = this.getClass();

            transformMethods = new HashMap<>();

            for(final Method method : clazz.getMethods()) {
                if(method.isAnnotationPresent(Transform.class)) {
                    final Transform annotation = method.getAnnotation(Transform.class);

                    Map<Class<?>, Method> from = transformMethods.get(annotation.from());
                    if(from == null) {
                        from = new HashMap<>();
                        transformMethods.put(annotation.from(), from);
                    }

                    from.put(annotation.to(), method);
                }
            }

            transformMethods = Collections.unmodifiableMap(transformMethods);
        }
    }

    @Override
    @SuppressWarnings("unchecked") // (T) cast will be safe as long as Transform to type matches return type
    public <F, T> T transform(final Class<F> fromType, final Class<T> toType, final F item, final PipelineContext context) {
        try {
            return (T)transformMethods().get(fromType).get(toType).invoke(this, item, context);
        } catch(final InvocationTargetException e) {
            if(e.getCause() instanceof RuntimeException) {
                throw (RuntimeException)e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        } catch(IllegalAccessException | IllegalArgumentException e) {
            LOGGER.error("Failed to invoke transform method from " + fromType.getCanonicalName() + " to " + toType.getCanonicalName() + ".", e);
            throw new RuntimeException(e);
        }
    }

    private Map<Class<?>, Map<Class<?>, Method>> transformMethods() {
        if(transformMethods == null) {
            initialize();
        }

        return transformMethods;
    }

    @Override
    public Map<Class<?>, Set<Class<?>>> transforms() {
        final Map<Class<?>, Set<Class<?>>> result = new HashMap<>();
        for(final Class<?> from : transformMethods().keySet()) {
            result.put(from, transformMethods().get(from).keySet());
        }
        return Collections.unmodifiableMap(result);
    }
}
