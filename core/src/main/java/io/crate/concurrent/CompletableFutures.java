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

import com.google.common.util.concurrent.Uninterruptibles;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public final class CompletableFutures {

    private CompletableFutures() {
    }

    /**
     * Creates a new {@code CompletableFuture} whose value is a list containing the
     * values of all its input futures, if all succeed (maintaining the order of the inputs).
     * If any input future fails, the returned future fails immediately.
     * <p>
     * Equivalent to guava's Futures.allAsList adapted to CompletableFuture
     */
    public static <T> CompletableFuture<List<T>> allSuccessfulOrFailedFuture(List<CompletableFuture<T>> futures) {
        List<T> collectedResults = new ArrayList<>(futures.size());
        Throwable futureException = null;
        for (CompletableFuture<? extends T> future : futures) {
            try {
                collectedResults.add(Uninterruptibles.getUninterruptibly(future));
            } catch (ExecutionException t) {
                futureException = t.getCause();
                break;
            }
        }
        if (futureException != null) {
            CompletableFuture<List<T>> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(futureException);
            return failedFuture;
        } else {
            return CompletableFuture.completedFuture(collectedResults);
        }
    }

    /**
     * Creates a new {@code CompletableFuture} whose value is a list containing the results of
     * all successful input futures, maintaining the order of the inputs.
     * If a future fails or is cancelled, null is returned on its corresponding position.
     * <p>
     * Equivalent to guava's Futures.successfulAsList, adapted to CompletableFuture
     */
    public static <T> CompletableFuture<List<T>> successfulAsList(List<CompletableFuture<T>> futures) {
        List<T> collectedResults = new ArrayList<>(futures.size());
        for (CompletableFuture<? extends T> future : futures) {
            try {
                collectedResults.add(Uninterruptibles.getUninterruptibly(future));
            } catch (ExecutionException e) {
                collectedResults.add(null);
            }
        }
        return CompletableFuture.completedFuture(collectedResults);
    }

    /**
     * Creates a {@code CompletableFuture} which will throw the given exception on the invocations
     * of get()
     */
    public static <T> CompletableFuture<T> failedFuture(Throwable t) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(t);
        return future;
    }

}
