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
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;

public interface Stream<T> {

    public static class Wrapper<T> implements Iterator<T> {

        private final Queue<T> next = new LinkedList<T>();
        private final Stream<T> stream;

        public Wrapper(Stream<T> stream) {
            this.stream = stream;
        }

        @Override
        public boolean hasNext() {
            if (!next.isEmpty()){
                return true;
            }

            T nextItem = stream.next();

            if (nextItem == null){
                return false;
            }

            next.add(nextItem);

            return true;
        }

        @Override
        public T next() {
            if (next.isEmpty()){
                if (!hasNext()){
                    throw new NoSuchElementException();
                }
            }

            return next.poll();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public T next();

}
