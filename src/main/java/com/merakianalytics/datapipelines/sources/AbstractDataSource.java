package com.merakianalytics.datapipelines.sources;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.merakianalytics.datapipelines.NotSupportedException;
import com.merakianalytics.datapipelines.PipelineContext;
import com.merakianalytics.datapipelines.iterators.CloseableIterator;

/**
 * A base class for easily defining new {@link com.merakianalytics.datapipelines.sources.DataSource}s using just the
 * {@link com.merakianalytics.datapipelines.sources.Get} and {@link com.merakianalytics.datapipelines.sources.GetMany} annotations.
 *
 * Methods annotated with these annotations in subclasses will be automatically picked up as delegate methods for
 * {@link com.merakianalytics.datapipelines.sources.AbstractDataSource#get(Class, Map, PipelineContext)} and
 * {@link com.merakianalytics.datapipelines.sources.AbstractDataSource#getMany(Class, Map, PipelineContext)}.
 *
 * The {@link com.merakianalytics.datapipelines.sources.AbstractDataSource#provides()} functionality will be automatically determined from the annotations as
 * well,
 * with {@link com.merakianalytics.datapipelines.sources.AbstractDataSource#ignore()} allowing subclasses to alter this at runtime.
 *
 * @see com.merakianalytics.datapipelines.sources.Get
 * @see com.merakianalytics.datapipelines.sources.GetMany
 */
public abstract class AbstractDataSource implements DataSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDataSource.class);
    private Map<Class<?>, Method> getManyMethods;
    private Map<Class<?>, Method> getMethods;
    private final Object initLock = new Object();

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(final Class<T> type, final Map<String, Object> query, final PipelineContext context) {
        try {
            final Method method = getMethods().get(type);

            if(method == null) {
                throw new NotSupportedException(type.getCanonicalName() + " is not supported by this DataSource!");
            }

            return (T)method.invoke(this, query, context);
        } catch(final InvocationTargetException e) {
            if(e.getCause() instanceof RuntimeException) {
                throw (RuntimeException)e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        } catch(IllegalAccessException | IllegalArgumentException e) {
            LOGGER.error("Failed to invoke get method for " + type.getCanonicalName() + ".", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> CloseableIterator<T> getMany(final Class<T> type, final Map<String, Object> query, final PipelineContext context) {
        try {
            final Method method = getManyMethods().get(type);

            if(method == null) {
                throw new NotSupportedException(type.getCanonicalName() + " is not supported by this DataSource!");
            }

            return (CloseableIterator<T>)method.invoke(this, query, context);
        } catch(final InvocationTargetException e) {
            if(e.getCause() instanceof RuntimeException) {
                throw (RuntimeException)e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        } catch(IllegalAccessException | IllegalArgumentException e) {
            LOGGER.error("Failed to invoke getMany method for " + type.getCanonicalName() + ".", e);
            throw new RuntimeException(e);
        }
    }

    private Map<Class<?>, Method> getManyMethods() {
        if(getManyMethods == null) {
            initialize();
        }

        return getManyMethods;
    }

    private Map<Class<?>, Method> getMethods() {
        if(getMethods == null) {
            initialize();
        }

        return getMethods;
    }

    /**
     * @return any classes which may exist in {@link com.merakianalytics.datapipelines.sources.Get} and
     *         {@link com.merakianalytics.datapipelines.sources.GetMany} annotations but should be ignored by this
     *         {@link com.merakianalytics.datapipelines.sources.AbstractDataSource} instance
     */
    protected Set<Class<?>> ignore() {
        return Collections.emptySet();
    }

    private void initialize() {
        synchronized(initLock) {
            if(getMethods != null && getManyMethods != null) {
                return;
            }

            final Class<? extends AbstractDataSource> clazz = this.getClass();
            final Set<Class<?>> ignore = ignore();

            getMethods = new HashMap<>();
            getManyMethods = new HashMap<>();

            for(final Method method : clazz.getMethods()) {
                if(method.isAnnotationPresent(Get.class)) {
                    final Get annotation = method.getAnnotation(Get.class);
                    if(!ignore.contains(annotation.value())) {
                        getMethods.put(annotation.value(), method);
                    }
                }

                if(method.isAnnotationPresent(GetMany.class)) {
                    final GetMany annotation = method.getAnnotation(GetMany.class);
                    if(!ignore.contains(annotation.value())) {
                        getManyMethods.put(annotation.value(), method);
                    }
                }
            }

            getMethods = Collections.unmodifiableMap(getMethods);
            getManyMethods = Collections.unmodifiableMap(getManyMethods);
        }
    }

    @Override
    public Set<Class<?>> provides() {
        final Set<Class<?>> provides = new HashSet<>();
        provides.addAll(getMethods().keySet());
        provides.addAll(getManyMethods().keySet());
        return Collections.unmodifiableSet(provides);
    }
}
