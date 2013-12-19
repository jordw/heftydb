/*
 * Copyright (c) 2013. Jordan Williams
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jordanwilliams.heftydb.util;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class FilteringIterator<T> implements Iterator<T> {

    public interface Filter<T> {
        public T next(Iterator<T> delegate);
    }

    private final Queue<T> next = new LinkedList<T>();
    private final Filter<T> filter;
    private final Iterator<T> delegate;

    public FilteringIterator(Filter<T> filter, Iterator<T> delegate) {
        this.filter = filter;
        this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        if (!next.isEmpty()) {
            return true;
        }

        T nextCandidate = filter.next(delegate);

        if (nextCandidate == null) {
            return false;
        }

        next.add(nextCandidate);

        return true;
    }

    @Override
    public T next() {
        if (next.isEmpty()) {
            hasNext();
        }

        return next.poll();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
