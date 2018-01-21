package com.merakianalytics.datapipelines.sinks;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.merakianalytics.datapipelines.PipelineContext;

/**
 * A base class for easily defining new {@link com.merakianalytics.datapipelines.sinks.DataSink}s using just the
 * {@link com.merakianalytics.datapipelines.sinks.Put} and {@link com.merakianalytics.datapipelines.sinks.PutMany} annotations.
 *
 * Methods annotated with these annotations in subclasses will be automatically picked up as delegate methods for
 * {@link com.merakianalytics.datapipelines.sinks.AbstractDataSink#put(Class, Object, PipelineContext)} and
 * {@link com.merakianalytics.datapipelines.sinks.AbstractDataSink#putMany(Class, Iterable, PipelineContext)}.
 *
 * The {@link com.merakianalytics.datapipelines.sinks.AbstractDataSink#accepts()} functionality will be automatically determined from the annotations as well,
 * with {@link com.merakianalytics.datapipelines.sinks.AbstractDataSink#ignore()} allowing subclasses to alter this at runtime.
 *
 * @see com.merakianalytics.datapipelines.sinks.Put
 * @see com.merakianalytics.datapipelines.sinks.PutMany
 */
public abstract class AbstractDataSink implements DataSink {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDataSink.class);
    private final Object initLock = new Object();
    private Map<Class<?>, Method> putManyMethods;
    private Map<Class<?>, Method> putMethods;

    @Override
    public Set<Class<?>> accepts() {
        final Set<Class<?>> provides = new HashSet<>();
        provides.addAll(putMethods().keySet());
        provides.addAll(putManyMethods().keySet());
        return Collections.unmodifiableSet(provides);
    }

    /**
     * @return any classes which may exist in {@link com.merakianalytics.datapipelines.sinks.Put} and {@link com.merakianalytics.datapipelines.sinks.PutMany}
     *         annotations but should be ignored by this {@link com.merakianalytics.datapipelines.sinks.AbstractDataSink} instance
     */
    protected Set<Class<?>> ignore() {
        return Collections.emptySet();
    }

    private void initialize() {
        synchronized(initLock) {
            if(putMethods != null && putManyMethods != null) {
                return;
            }

            final Class<? extends AbstractDataSink> clazz = this.getClass();
            final Set<Class<?>> ignore = ignore();

            putMethods = new HashMap<>();
            putManyMethods = new HashMap<>();

            for(final Method method : clazz.getMethods()) {
                if(method.isAnnotationPresent(Put.class)) {
                    final Put annotation = method.getAnnotation(Put.class);
                    if(!ignore.contains(annotation.annotationType())) {
                        putMethods.put(annotation.value(), method);
                    }
                }

                if(method.isAnnotationPresent(PutMany.class)) {
                    final PutMany annotation = method.getAnnotation(PutMany.class);
                    if(!ignore.contains(annotation.annotationType())) {
                        putManyMethods.put(annotation.value(), method);
                    }
                }
            }

            putMethods = Collections.unmodifiableMap(putMethods);
            putManyMethods = Collections.unmodifiableMap(putManyMethods);
        }
    }

    @Override
    public <T> void put(final Class<T> type, final T item, final PipelineContext context) {
        try {
            putMethods().get(type).invoke(this, item, context);
        } catch(final InvocationTargetException e) {
            if(e.getCause() instanceof RuntimeException) {
                throw (RuntimeException)e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        } catch(IllegalAccessException | IllegalArgumentException e) {
            LOGGER.error("Failed to invoke put method for " + type.getCanonicalName() + ".", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> void putMany(final Class<T> type, final Iterable<T> items, final PipelineContext context) {
        try {
            putManyMethods().get(type).invoke(this, items, context);
        } catch(final InvocationTargetException e) {
            if(e.getCause() instanceof RuntimeException) {
                throw (RuntimeException)e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        } catch(IllegalAccessException | IllegalArgumentException e) {
            LOGGER.error("Failed to invoke putMany method for " + type.getCanonicalName() + ".", e);
            throw new RuntimeException(e);
        }
    }

    private Map<Class<?>, Method> putManyMethods() {
        if(putManyMethods == null) {
            initialize();
        }

        return putManyMethods;
    }

    private Map<Class<?>, Method> putMethods() {
        if(putMethods == null) {
            initialize();
        }

        return putMethods;
    }
}
