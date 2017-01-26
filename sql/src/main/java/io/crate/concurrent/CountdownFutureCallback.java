/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.concurrent;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A future acting as a FutureCallback. It is set when once numCalls have been made to the callback.
 * If a failure occurs the last failure will be used as exception. The result is always null.
 */
public class CountdownFutureCallback extends CompletableFuture<Void> {

    private final AtomicInteger counter;
    private final AtomicReference<Throwable> lastFailure = new AtomicReference<>();

    public CountdownFutureCallback(int numCalls) {
        counter = new AtomicInteger(numCalls);
    }

    public void onSuccess(@Nullable Object result) {
        countdown();
    }

    public void onFailure(@Nonnull Throwable t) {
        lastFailure.set(t);
        countdown();
    }

    private void countdown() {
        if (counter.decrementAndGet() == 0) {
            Throwable throwable = lastFailure.get();
            if (throwable == null) {
                complete(null);
            } else {
                completeExceptionally(throwable);
            }
        }
    }
}
