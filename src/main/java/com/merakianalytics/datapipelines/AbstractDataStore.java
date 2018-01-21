package com.merakianalytics.datapipelines;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.merakianalytics.datapipelines.iterators.CloseableIterator;
import com.merakianalytics.datapipelines.sinks.Put;
import com.merakianalytics.datapipelines.sinks.PutMany;
import com.merakianalytics.datapipelines.sources.Get;
import com.merakianalytics.datapipelines.sources.GetMany;

/**
 * A base class for easily defining new {@link com.merakianalytics.datapipelines.DataStore}s using just the
 * {@link com.merakianalytics.datapipelines.sources.Get}, {@link com.merakianalytics.datapipelines.sources.GetMany},
 * {@link com.merakianalytics.datapipelines.sinks.Put}, and {@link com.merakianalytics.datapipelines.sinks.PutMany} annotations.
 *
 * Methods annotated with these annotations in subclasses will be automatically picked up as delegate methods for
 * {@link com.merakianalytics.datapipelines.AbstractDataStore#get(Class, Map, PipelineContext)},
 * {@link com.merakianalytics.datapipelines.AbstractDataStore#getMany(Class, Map, PipelineContext)},
 * {@link com.merakianalytics.datapipelines.AbstractDataStore#put(Class, Object, PipelineContext)}, and
 * {@link com.merakianalytics.datapipelines.AbstractDataStore#putMany(Class, Iterable, PipelineContext)}.
 *
 * The {@link com.merakianalytics.datapipelines.AbstractDataStore#provides()} and {@link com.merakianalytics.datapipelines.AbstractDataStore#accepts()}
 * functionality will be automatically determined from the annotations as well, with {@link com.merakianalytics.datapipelines.AbstractDataStore#ignore()}
 * allowing subclasses to alter this at runtime.
 *
 * @see com.merakianalytics.datapipelines.sources.Get
 * @see com.merakianalytics.datapipelines.sources.GetMany
 * @see com.merakianalytics.datapipelines.sinks.Put
 * @see com.merakianalytics.datapipelines.sinks.PutMany
 */
public abstract class AbstractDataStore implements DataStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDataStore.class);
    private Map<Class<?>, Method> getManyMethods;
    private Map<Class<?>, Method> getMethods;
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
        synchronized(initLock) {
            if(getManyMethods == null) {
                initialize();
            }
        }
        return getManyMethods;
    }

    private Map<Class<?>, Method> getMethods() {
        synchronized(initLock) {
            if(getMethods == null) {
                initialize();
            }
        }
        return getMethods;
    }

    /**
     * @return any classes which may exist in {@link com.merakianalytics.datapipelines.sources.Get}, {@link com.merakianalytics.datapipelines.sources.GetMany},
     *         {@link com.merakianalytics.datapipelines.sinks.Put}, and {@link com.merakianalytics.datapipelines.sinks.PutMany} annotations but should be
     *         ignored by this {@link com.merakianalytics.datapipelines.AbstractDataStore} instance
     */
    protected Set<Class<?>> ignore() {
        return Collections.emptySet();
    }

    private void initialize() {
        synchronized(initLock) {
            if(getMethods != null && getManyMethods != null && putMethods != null && putManyMethods != null) {
                return;
            }

            final Class<? extends AbstractDataStore> clazz = this.getClass();
            final Set<Class<?>> ignore = ignore();

            getMethods = new HashMap<>();
            getManyMethods = new HashMap<>();

            putMethods = new HashMap<>();
            putManyMethods = new HashMap<>();

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

                if(method.isAnnotationPresent(Put.class)) {
                    final Put annotation = method.getAnnotation(Put.class);
                    if(!ignore.contains(annotation.value())) {
                        putMethods.put(annotation.value(), method);
                    }
                }

                if(method.isAnnotationPresent(PutMany.class)) {
                    final PutMany annotation = method.getAnnotation(PutMany.class);
                    if(!ignore.contains(annotation.value())) {
                        putManyMethods.put(annotation.value(), method);
                    }
                }
            }

            getMethods = Collections.unmodifiableMap(getMethods);
            getManyMethods = Collections.unmodifiableMap(getManyMethods);

            putMethods = Collections.unmodifiableMap(putMethods);
            putManyMethods = Collections.unmodifiableMap(putManyMethods);
        }
    }

    @Override
    public Set<Class<?>> provides() {
        final Set<Class<?>> provides = new HashSet<>();
        provides.addAll(getMethods().keySet());
        provides.addAll(getManyMethods().keySet());
        return Collections.unmodifiableSet(provides);
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
        synchronized(initLock) {
            if(putManyMethods == null) {
                initialize();
            }
        }
        return putManyMethods;
    }

    private Map<Class<?>, Method> putMethods() {
        synchronized(initLock) {
            if(putMethods == null) {
                initialize();
            }
        }

        return putMethods;
    }
}
