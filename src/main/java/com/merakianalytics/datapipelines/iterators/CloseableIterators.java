package com.merakianalytics.datapipelines.iterators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.base.Function;

/**
 * Utilities for creating {@link com.merakianalytics.datapipelines.iterators.CloseableIterator}s
 *
 * @see com.merakianalytics.datapipelines.iterators.CloseableIterator
 */
public abstract class CloseableIterators {
    /**
     * @param <T>
     *        the type of the {@link com.merakianalytics.datapipelines.iterators.CloseableIterator}
     * @return an empty {@link com.merakianalytics.datapipelines.iterators.CloseableIterator}
     */
    public static <T> CloseableIterator<T> empty() {
        return from(Collections.<T> emptyIterator());
    }

    /**
     * Creates a {@link com.merakianalytics.datapipelines.iterators.CloseableIterator} with no close behavior over the provided data
     *
     * @param <T>
     *        the type of the {@link java.lang.Iterable}
     * @param iterable
     *        the data to get a {@link com.merakianalytics.datapipelines.iterators.CloseableIterator} for
     * @return a {@link com.merakianalytics.datapipelines.iterators.CloseableIterator} which iterates the data
     */
    public static <T> CloseableIterator<T> from(final Iterable<T> iterable) {
        return from(iterable.iterator());
    }

    /**
     * Creates a {@link com.merakianalytics.datapipelines.iterators.CloseableIterator} wrapper around the provided {@link java.util.Iterator}
     *
     * @param <T>
     *        the type of the {@link java.util.Iterator}
     * @param iterator
     *        the {@link java.util.Iterator} to wrap in a {@link com.merakianalytics.datapipelines.iterators.CloseableIterator}
     * @return a {@link com.merakianalytics.datapipelines.iterators.CloseableIterator} view of the {@link java.util.Iterator}
     */
    public static <T> CloseableIterator<T> from(final Iterator<T> iterator) {
        if(iterator instanceof CloseableIterator) {
            return (CloseableIterator<T>)iterator;
        }

        return new CloseableIterator<T>() {
            @Override
            public void close() {
                // Do nothing
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public T next() {
                return iterator.next();
            }

            @Override
            public void remove() {
                iterator.remove();
            }
        };
    }

    /**
     * Creates a {@link com.merakianalytics.datapipelines.iterators.LazyList} from a {@link com.merakianalytics.datapipelines.iterators.CloseableIterator}.
     * Elements will be lazily pulled from the {@link java.util.Iterator} for use in the list.
     *
     * @param <T>
     *        the type of the {@link com.merakianalytics.datapipelines.iterators.CloseableIterator}
     * @param iterator
     *        the {@link com.merakianalytics.datapipelines.iterators.CloseableIterator} to create the
     *        {@link com.merakianalytics.datapipelines.iterators.LazyList} from
     * @return a {@link com.merakianalytics.datapipelines.iterators.LazyList} which contains data loaded from the
     *         {@link com.merakianalytics.datapipelines.iterators.CloseableIterator}
     */
    public static <T> LazyList<T> toLazyList(final CloseableIterator<T> iterator) {
        return new LazyList<>(iterator);
    }

    /**
     * Exhausts the {@link com.merakianalytics.datapipelines.iterators.CloseableIterator} to populate a {@link java.util.List}. The
     * {@link com.merakianalytics.datapipelines.iterators.CloseableIterator} will be closed once exhausted.
     *
     * @param <T>
     *        the type of the {@link com.merakianalytics.datapipelines.iterators.CloseableIterator}
     * @param iterator
     *        the {@link com.merakianalytics.datapipelines.iterators.CloseableIterator} to populate the {@link java.util.List} with
     * @return a {@link java.util.List}
     */
    public static <T> List<T> toList(final CloseableIterator<T> iterator) {
        final List<T> list = new ArrayList<>();
        while(iterator.hasNext()) {
            list.add(iterator.next());
        }
        iterator.close();
        return list;
    }

    /**
     * Exhausts the {@link com.merakianalytics.datapipelines.iterators.CloseableIterator} to populate a {@link java.util.Set}. The
     * {@link com.merakianalytics.datapipelines.iterators.CloseableIterator} will be closed once exhausted.
     *
     * @param <T>
     *        the type of the {@link com.merakianalytics.datapipelines.iterators.CloseableIterator}
     * @param iterator
     *        the {@link com.merakianalytics.datapipelines.iterators.CloseableIterator} to populate the {@link java.util.Set} with
     * @return a {@link java.util.Set}
     */
    public static <T> Set<T> toSet(final CloseableIterator<T> iterator) {
        final Set<T> set = new HashSet<>();
        while(iterator.hasNext()) {
            set.add(iterator.next());
        }
        iterator.close();
        return set;
    }

    /**
     * Creates a {@link com.merakianalytics.datapipelines.iterators.CloseableIterator} which applies the provided conversion to each element as they are
     * returned
     *
     * @param <F>
     *        the type of the {@link java.util.Iterator}
     * @param <T>
     *        the type of the resulting {@link com.merakianalytics.datapipelines.iterators.CloseableIterator}
     * @param iterator
     *        the {@link java.util.Iterator} to wrap in the transforming {@link com.merakianalytics.datapipelines.iterators.CloseableIterator}
     * @param conversion
     *        the conversion to apply to each element
     * @return a {@link com.merakianalytics.datapipelines.iterators.CloseableIterator} which transforms the elements of the provided {@link java.util.Iterator}
     *         as they are requested
     */
    public static <F, T> CloseableIterator<T> transform(final Iterator<F> iterator, final Function<F, T> conversion) {
        if(iterator instanceof CloseableIterator) {
            final CloseableIterator<F> iter = (CloseableIterator<F>)iterator;
            return new CloseableIterator<T>() {
                @Override
                public void close() {
                    iter.close();
                }

                @Override
                public boolean hasNext() {
                    return iter.hasNext();
                }

                @Override
                public T next() {
                    return conversion.apply(iter.next());
                }

                @Override
                public void remove() {
                    iter.remove();
                }
            };
        } else {
            return new CloseableIterator<T>() {
                @Override
                public void close() {}

                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public T next() {
                    return conversion.apply(iterator.next());
                }

                @Override
                public void remove() {
                    iterator.remove();
                }
            };
        }
    }
}
