package com.merakianalytics.datapipelines.iterators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

/**
 * A {@link java.util.List} which lazily pulls its elements from a {@link com.merakianalytics.datapipelines.iterators.CloseableIterator} as they are needed
 *
 * @see com.merakianalytics.datapipelines.iterators.CloseableIterator
 */
public class LazyList<T> implements List<T>, AutoCloseable {
    private class LazyListIterator implements ListIterator<T> {
        private int index;

        public LazyListIterator() {
            index = -1;
        }

        public LazyListIterator(final int index) {
            load(index);
            if(index > data.size() - 1) {
                throw new IndexOutOfBoundsException("Index: " + index);
            }
            this.index = index;
        }

        @Override
        public void add(final T item) {
            if(!closed) {
                synchronized(dataSource) {
                    data.add(index, item);
                }
            } else {
                data.add(index, item);
            }
        }

        @Override
        public boolean hasNext() {
            if(index + 2 <= data.size()) {
                return true;
            }
            if(closed) {
                return false;
            }
            synchronized(dataSource) {
                if(!closed) {
                    return dataSource.hasNext();
                }
            }
            return false;
        }

        @Override
        public boolean hasPrevious() {
            return index >= 1;
        }

        @Override
        public T next() {
            if(!hasNext()) {
                throw new NoSuchElementException();
            }

            index += 1;
            load(index);
            return data.get(index);
        }

        @Override
        public int nextIndex() {
            return index + 1;
        }

        @Override
        public T previous() {
            if(!hasPrevious()) {
                throw new NoSuchElementException();
            }

            index -= 1;
            return data.get(index);
        }

        @Override
        public int previousIndex() {
            return index - 1;
        }

        @Override
        public void remove() {
            if(index < 0) {
                throw new IllegalStateException();
            }

            data.remove(index);
        }

        @Override
        public void set(final T item) {
            if(index < 0) {
                throw new IllegalStateException();
            }

            data.set(index, item);
        }

    }

    private boolean closed;
    private final List<T> data;
    private final CloseableIterator<T> dataSource;

    /**
     * @param dataSource
     *        the {@link com.merakianalytics.datapipelines.iterators.CloseableIterator} to lazily load data from
     */
    public LazyList(final CloseableIterator<T> dataSource) {
        data = new ArrayList<>();
        this.dataSource = dataSource;
    }

    /**
     * @param dataSource
     *        {@link com.merakianalytics.datapipelines.iterators.CloseableIterator}
     * @param initialCapacity
     *        the expected number of elements that will be provided by the {@link com.merakianalytics.datapipelines.iterators.CloseableIterator}.
     */
    public LazyList(final CloseableIterator<T> dataSource, final int initialCapacity) {
        data = new ArrayList<>(initialCapacity);
        this.dataSource = dataSource;
    }

    /**
     * @param items
     *        the data to seed the list with before lazily loading data from the provided {@link com.merakianalytics.datapipelines.iterators.CloseableIterator}
     * @param dataSource
     *        the {@link com.merakianalytics.datapipelines.iterators.CloseableIterator} to lazily load data from
     */
    public LazyList(final Collection<? extends T> items, final CloseableIterator<T> dataSource) {
        data = new ArrayList<>(items);
        this.dataSource = dataSource;
    }

    @Override
    public void add(final int index, final T item) {
        load(index);
        data.add(index, item);
    }

    @Override
    public boolean add(final T item) {
        load();
        return data.add(item);
    }

    @Override
    public boolean addAll(final Collection<? extends T> items) {
        load();
        return data.addAll(items);
    }

    @Override
    public boolean addAll(final int index, final Collection<? extends T> items) {
        load(index);
        return data.addAll(index, items);
    }

    @Override
    public void clear() {
        close();
        data.clear();
    }

    @Override
    public void close() {
        if(!closed) {
            synchronized(dataSource) {
                if(!closed) {
                    dataSource.close();
                    closed = true;
                }
            }
        }
    }

    @Override
    public boolean contains(final Object item) {
        return indexOf(item) != -1;
    }

    @Override
    public boolean containsAll(final Collection<?> items) {
        for(final Object item : items) {
            if(indexOf(item) == -1) {
                return false;
            }
        }
        return true;
    }

    @Override
    public T get(final int index) {
        load(index);
        return data.get(index);
    }

    @Override
    public int indexOf(final Object item) {
        final int index = data.indexOf(item);
        if(index != -1) {
            return index;
        }

        if(!closed) {
            synchronized(dataSource) {
                if(!closed) {
                    while(dataSource.hasNext()) {
                        final T next = dataSource.next();
                        data.add(next);

                        if(item == null && next == null) {
                            return data.size() - 1;
                        } else if(item == null || next == null) {
                            continue;
                        } else if(item.equals(next)) {
                            return data.size() - 1;
                        }
                    }

                    close();
                }
            }
        }
        return -1;
    }

    @Override
    public boolean isEmpty() {
        if(!data.isEmpty()) {
            return false;
        }
        if(closed) {
            return true;
        }
        synchronized(dataSource) {
            if(!closed) {
                return !dataSource.hasNext();
            }
        }
        return true;
    }

    @Override
    public Iterator<T> iterator() {
        return new LazyListIterator();
    }

    @Override
    public int lastIndexOf(final Object item) {
        load();
        return data.lastIndexOf(item);
    }

    @Override
    public ListIterator<T> listIterator() {
        return new LazyListIterator();
    }

    @Override
    public ListIterator<T> listIterator(final int index) {
        return new LazyListIterator(index);
    }

    private void load() {
        load(Integer.MAX_VALUE);
    }

    private void load(final int toIndex) {
        if(!closed && toIndex > data.size() - 1) {
            synchronized(dataSource) {
                if(!closed) {
                    while(toIndex > data.size() - 1 && dataSource.hasNext()) {
                        data.add(dataSource.next());
                    }

                    // In this case we exited because hasNext was false. We use this check instead to avoid any side effects of calling hasNext again.
                    if(toIndex > data.size() - 1) {
                        close();
                    }
                }
            }
        }
    }

    @Override
    public T remove(final int index) {
        load(index);
        return data.remove(index);
    }

    @Override
    public boolean remove(final Object item) {
        final int index = indexOf(item);
        if(index == -1) {
            return false;
        }
        data.remove(index);
        return true;
    }

    @Override
    public boolean removeAll(final Collection<?> items) {
        load();
        return data.removeAll(items);
    }

    @Override
    public boolean retainAll(final Collection<?> items) {
        load();
        return data.retainAll(items);
    }

    @Override
    public T set(final int index, final T item) {
        load(index);
        return data.set(index, item);
    }

    @Override
    public int size() {
        load();
        return data.size();
    }

    @Override
    public List<T> subList(final int fromIndex, final int toIndex) {
        load(toIndex);
        return data.subList(fromIndex, toIndex);
    }

    @Override
    public Object[] toArray() {
        load();
        return data.toArray();
    }

    @SuppressWarnings("hiding")
    @Override
    public <T> T[] toArray(final T[] array) {
        load();
        return data.toArray(array);
    }
}