package com.merakianalytics.datapipelines.iterators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.base.Function;

public abstract class CloseableIterators {
    public static <T> CloseableIterator<T> empty() {
        return from(Collections.<T> emptyIterator());
    }

    public static <T> CloseableIterator<T> from(final Iterable<T> iterable) {
        return from(iterable.iterator());
    }

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

    public static <T> LazyList<T> toLazyList(final CloseableIterator<T> iterator) {
        return new LazyList<>(iterator);
    }

    public static <T> List<T> toList(final CloseableIterator<T> iterator) {
        final List<T> list = new ArrayList<>();
        while(iterator.hasNext()) {
            list.add(iterator.next());
        }
        iterator.close();
        return list;
    }

    public static <T> Set<T> toSet(final CloseableIterator<T> iterator) {
        final Set<T> set = new HashSet<>();
        while(iterator.hasNext()) {
            set.add(iterator.next());
        }
        iterator.close();
        return set;
    }

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
                    return null;
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
                    return null;
                }

                @Override
                public void remove() {
                    iterator.remove();
                }
            };
        }
    }
}
