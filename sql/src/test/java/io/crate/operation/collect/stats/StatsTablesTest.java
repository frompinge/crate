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

package io.crate.operation.collect.stats;

import com.google.common.collect.ImmutableList;
import io.crate.breaker.*;
import io.crate.metadata.settings.CrateSettings;
import io.crate.operation.reference.sys.job.JobContext;
import io.crate.operation.reference.sys.job.JobContextLog;
import io.crate.operation.reference.sys.operation.OperationContext;
import io.crate.operation.reference.sys.operation.OperationContextLog;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.hamcrest.core.Is.is;

public class StatsTablesTest extends CrateUnitTest {

    private ScheduledExecutorService scheduler;
    private static CrateCircuitBreakerService breakerService;
    private RamAccountingContext ramAccountingContext;

    @Before
    public void createScheduler() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        ramAccountingContext = new RamAccountingContext("testRamAccountingContext",
            breakerService.getBreaker(CrateCircuitBreakerService.LOGS));
    }

    @After
    public void terminateScheduler() throws InterruptedException {
        terminate(scheduler);
    }

    @BeforeClass
    public static void beforeClass() {
        NodeSettingsService settingsService = new NodeSettingsService(Settings.EMPTY);
        CircuitBreakerService esBreakerService = new HierarchyCircuitBreakerService(Settings.EMPTY, settingsService);
        breakerService = new CrateCircuitBreakerService(
            Settings.EMPTY, settingsService, esBreakerService);
    }

    @Test
    public void testSettingsChanges() {
        NodeSettingsService nodeSettingsService = new NodeSettingsService(Settings.EMPTY);
        StatsTablesService stats = new StatsTablesService(Settings.EMPTY, nodeSettingsService, scheduler, breakerService);

        assertThat(stats.isEnabled(), is(false));
        assertThat(stats.lastJobsLogSize, is(CrateSettings.STATS_JOBS_LOG_SIZE.defaultValue()));
        assertThat(stats.lastOperationsLogSize, is(CrateSettings.STATS_OPERATIONS_LOG_SIZE.defaultValue()));

        // even though logSizes are > 0 it must be a NoopQueue because the stats are disabled
        assertThat(stats.jobsLogSink, Matchers.instanceOf(NoopLogSink.class));
        Settings settings = Settings.builder()
            .put(CrateSettings.STATS_ENABLED.settingName(), true)
            .put(CrateSettings.STATS_JOBS_LOG_SIZE.settingName(), 100)
            .put(CrateSettings.STATS_OPERATIONS_LOG_SIZE.settingName(), 100).build();
        stats = new StatsTablesService(settings, nodeSettingsService, scheduler, breakerService);

        stats.listener.onRefreshSettings(Settings.builder()
            .put(CrateSettings.STATS_OPERATIONS_LOG_SIZE.settingName(), 200).build());

        assertThat(stats.isEnabled(), is(true));
        assertThat(stats.lastJobsLogSize, is(100));
        assertThat(stats.lastOperationsLogSize, is(200));
        assertThat(stats.jobsLogSink, Matchers.instanceOf(RamAccountingLogSink.class));

        // switch jobs_log queue
        stats.listener.onRefreshSettings(Settings.builder()
            .put(CrateSettings.STATS_JOBS_LOG_EXPIRATION.settingName(), "10s").build());
        assertThat(stats.jobsLogSink, Matchers.instanceOf(TimeExpiringRamAccountingLogSink.class));

        stats.listener.onRefreshSettings(Settings.builder()
            .put(CrateSettings.STATS_JOBS_LOG_SIZE.settingName(), 0)
            .put(CrateSettings.STATS_JOBS_LOG_EXPIRATION.settingName(), "0s").build());
        assertThat(stats.jobsLogSink, Matchers.instanceOf(NoopLogSink.class));

        stats.listener.onRefreshSettings(Settings.builder()
            .put(CrateSettings.STATS_JOBS_LOG_EXPIRATION.settingName(), "10s").build());
        assertThat(stats.jobsLogSink, Matchers.instanceOf(TimeExpiringRamAccountingLogSink.class));

        // logs got wiped:
        stats.listener.onRefreshSettings(Settings.builder()
            .put(CrateSettings.STATS_ENABLED.settingName(), false).build());
        assertThat(stats.jobsLogSink, Matchers.instanceOf(NoopLogSink.class));
        assertThat(stats.isEnabled(), is(false));
    }

    @Test
    public void testLogsArentWipedOnSizeChange() {
        NodeSettingsService nodeSettingsService = new NodeSettingsService(Settings.EMPTY);
        Settings settings = Settings.builder()
            .put(CrateSettings.STATS_ENABLED.settingName(), true).build();
        StatsTablesService stats = new StatsTablesService(settings, nodeSettingsService, scheduler, breakerService);

        stats.jobsLogSink.add(new JobContextLog(new JobContext(UUID.randomUUID(), "select 1", 1L), null));

        stats.listener.onRefreshSettings(Settings.builder()
            .put(CrateSettings.STATS_ENABLED.settingName(), true)
            .put(CrateSettings.STATS_JOBS_LOG_SIZE.settingName(), 200).build());

        assertThat(((RamAccountingLogSink) stats.jobsLogSink).size(), is(1));

        stats.operationsLogSink.add(new OperationContextLog(
            new OperationContext(1, UUID.randomUUID(), "foo", 2L), null));
        stats.operationsLogSink.add(new OperationContextLog(
            new OperationContext(1, UUID.randomUUID(), "foo", 3L), null));

        stats.listener.onRefreshSettings(Settings.builder()
            .put(CrateSettings.STATS_ENABLED.settingName(), true)
            .put(CrateSettings.STATS_OPERATIONS_LOG_SIZE.settingName(), 1).build());

        assertThat(((RamAccountingLogSink) stats.operationsLogSink).size(), is(1));
    }

    @Test
    public void testUniqueOperationIdsInOperationsTable() {
        StatsTables statsTables = new StatsTables(() -> true);
        statsTables.updateOperationsLog(new FixedSizeRamAccountingLogSink<>(ramAccountingContext, 10, StatsTablesService.OPERATION_CONTEXT_LOG_SIZE_ESTIMATOR::estimateSize));

        OperationContext ctxA = new OperationContext(0, UUID.randomUUID(), "dummyOperation", 1L);
        statsTables.operationStarted(ctxA.id, ctxA.jobId, ctxA.name);

        OperationContext ctxB = new OperationContext(0, UUID.randomUUID(), "dummyOperation", 1L);
        statsTables.operationStarted(ctxB.id, ctxB.jobId, ctxB.name);

        statsTables.operationFinished(ctxB.id, ctxB.jobId, null, -1);
        List<OperationContextLog> entries = ImmutableList.copyOf(statsTables.operationsLog.get());

        assertTrue(entries.contains(new OperationContextLog(ctxB, null)));
        assertFalse(entries.contains(new OperationContextLog(ctxA, null)));

        statsTables.operationFinished(ctxA.id, ctxA.jobId, null, -1);
        entries = ImmutableList.copyOf(statsTables.operationsLog.get());
        assertTrue(entries.contains(new OperationContextLog(ctxA, null)));
    }

}