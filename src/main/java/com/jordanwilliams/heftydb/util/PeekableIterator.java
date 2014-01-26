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

import java.util.Iterator;

public interface PeekableIterator<T> {

    public static class Wrapper<T> implements PeekableIterator<T> {

        private final Iterator<T> delegate;
        private T current;

        public Wrapper(Iterator<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public T current() {
            return current;
        }

        @Override
        public boolean advance() {
            if (!delegate.hasNext()){
                return false;
            }

            current = delegate.next();

            return true;
        }
    }

    public T current();

    public boolean advance();

}
