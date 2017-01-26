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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class FutureCompleteConsumerTest extends CrateUnitTest {

    @Test
    public void successConsumerIsCalledWhenThrowableIsNull() {
        String expectedResult = "success";
        FutureCompleteConsumer<String> completeConsumer = FutureCompleteConsumer.build(
            (String result) -> assertThat(result, is(expectedResult)),
            (Throwable t) -> fail("Expected the success consumer to be called.")
        );
        completeConsumer.accept(expectedResult, null);
    }

    @Test
    public void failureConsumerIsCalledWhenThrowableIsNotNull() {
        NullPointerException expectedException = new NullPointerException("bad");
        FutureCompleteConsumer<String> completeConsumer = FutureCompleteConsumer.build(
            (String result) -> fail("Expected the failure consumer to be called, but got result: " + result),
            (Throwable t) -> {
                assertThat(t, notNullValue());
                assertThat(t, is(expectedException));
            }
        );
        completeConsumer.accept("some result", expectedException);
    }
}
