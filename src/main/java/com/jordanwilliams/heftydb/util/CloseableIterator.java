/*
 * Copyright (c) 2014. Jordan Williams
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

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * An iterator that needs to be closed when it is no longer used so that off-heap memory or file handles can be
 * properly cleaned up.
 * @param <T>
 */
public interface CloseableIterator<T> extends Iterator<T>, Closeable {

    public static class Wrapper<T> implements CloseableIterator<T> {

        private final Iterator<T> delegate;

        public Wrapper(Iterator<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public T next() {
            return delegate.next();
        }

        @Override
        public void remove() {
            delegate.remove();
        }
    }

}
