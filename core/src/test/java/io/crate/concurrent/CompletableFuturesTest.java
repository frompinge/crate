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

import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class CompletableFuturesTest extends CrateUnitTest {

    @Test
    public void failedFutureIsCompletedExceptionally() {
        Exception exception = new Exception("failed future");
        CompletableFuture<Object> failedFuture = CompletableFutures.failedFuture(exception);
        assertThat(failedFuture.isCompletedExceptionally(), is(true));
    }

    @Test
    public void allSuccessfulOrFailedFutureWithSuccessfulFuturesAsInput() throws Exception {
        CompletableFuture<String> firstFuture = CompletableFuture.completedFuture("firstResult");
        CompletableFuture<String> secondFuture = CompletableFuture.completedFuture("secondResult");

        List<CompletableFuture<String>> futures = new ArrayList<>(2);
        futures.add(firstFuture);
        futures.add(secondFuture);

        CompletableFuture<List<String>> allSuccessfulResultsFuture =
            CompletableFutures.allSuccessfulOrFailedFuture(futures);

        List<String> results = allSuccessfulResultsFuture.get();
        assertThat(results.size(), is(2));
        assertThat(results.get(0), is("firstResult"));
        assertThat(results.get(1), is("secondResult"));
    }

    @Test
    public void allSuccessfulOrFailedFutureWithFailedFutureAmongstInputs() {
        CompletableFuture<String> firstFuture = CompletableFuture.completedFuture("firstResult");

        Exception futureException = new Exception("failed");
        CompletableFuture<String> secondFuture = new CompletableFuture<>();
        secondFuture.completeExceptionally(futureException);

        List<CompletableFuture<String>> futures = new ArrayList<>(2);
        futures.add(firstFuture);
        futures.add(secondFuture);

        CompletableFuture<List<String>> allSuccessfulResultsFuture =
            CompletableFutures.allSuccessfulOrFailedFuture(futures);

        try {
            allSuccessfulResultsFuture.get();
            fail("Expected the resulting future to be completed exceptionally");
        } catch (InterruptedException e) {
            fail("Expecting an ExecutionExecption");
        } catch (ExecutionException e) {
            assertThat(e.getCause(), is(futureException));
        }
    }
}
