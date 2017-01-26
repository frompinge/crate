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

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Consumer that runs when a CompletableFuture is complete. It delegates the calls to onSuccess/onFailure based
 * on the values received from the future - if the Throwable is null it calls the success consumer, otherwise it calls
 * the failure consumer.
 *
 * @param <R> the type of the expected result
 */
public class FutureCompleteConsumer<R> implements BiConsumer<R, Throwable> {

    private final Consumer<R> onSuccessConsumer;
    private final Consumer<Throwable> onFailureConsumer;

    public FutureCompleteConsumer(Consumer<R> onSuccessConsumer, Consumer<Throwable> onFailureConsumer) {
        if (onSuccessConsumer == null) {
            throw new IllegalArgumentException("onSuccessConsumer callback cannot be null");
        }
        if (onFailureConsumer == null) {
            throw new IllegalArgumentException("onFailureConsumer callback cannot be null");
        }
        this.onSuccessConsumer = onSuccessConsumer;
        this.onFailureConsumer = onFailureConsumer;
    }

    @Override
    public void accept(R result, Throwable t) {
        if (t == null) {
            onSuccessConsumer.accept(result);
        } else {
            onFailureConsumer.accept(t);
        }
    }

    /**
     * Delegates to the success consumer.
     * Visibility relaxed for programmatic use of the structure outside of the CompletableFuture context.
     */
    public void onSuccess(R result) {
        onSuccessConsumer.accept(result);
    }

    /**
     * Delegates to the failure consumer.
     * Visibility relaxed for programmatic use of the structure outside of the CompletableFuture context.
     */
    public void onFailure(Throwable t) {
        onFailureConsumer.accept(t);
    }

    public static <R> FutureCompleteConsumer<R> build(Consumer<R> onSuccessConsumer,
                                                      Consumer<Throwable> onFailureConsumer) {
        return new FutureCompleteConsumer<>(onSuccessConsumer, onFailureConsumer);
    }
}
