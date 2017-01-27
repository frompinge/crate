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

package io.crate.planner;

import com.carrotsearch.hppc.ObjectLongMap;
import com.google.common.collect.ImmutableList;
import io.crate.action.sql.Option;
import io.crate.action.sql.SQLOperations;
import io.crate.metadata.TableIdent;
import io.crate.plugin.SQLPlugin;
import io.crate.protocols.postgres.FormatCodes;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.anyByte;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class TableStatsServiceTest extends CrateDummyClusterServiceUnitTest {

    @Override
    protected Collection<Setting<?>> additionalClusterSettings() {
        return new SQLPlugin(Settings.EMPTY).getSettings();
    }

    @Test
    public void testRowsToTableStatConversion() {
        ObjectLongMap<TableIdent> stats = TableStatsService.statsFromRows(ImmutableList.of(
            new Object[]{1L, "custom", "foo"},
            new Object[]{2L, "doc", "foo"},
            new Object[]{3L, "bar", "foo"}));
        assertThat(stats.size(), is(3));
        assertThat(stats.get(new TableIdent("bar", "foo")), is(3L));
    }

    @Test
    public void testStatsQueriesCorrectly() throws Exception {
        final SQLOperations sqlOperations = mock(SQLOperations.class);
        SQLOperations.Session session = mock(SQLOperations.Session.class);
        when(sqlOperations.createSession(eq("sys"), eq(Option.NONE), eq(TableStatsService.DEFAULT_SOFT_LIMIT)))
            .thenReturn(session);

        TableStatsService statsService = new TableStatsService(
            Settings.EMPTY,
            THREAD_POOL,
            clusterService,
            () -> sqlOperations);
        statsService.run();

        verify(session, times(1)).parse(
            eq(TableStatsService.UNNAMED),
            eq(TableStatsService.STMT),
            eq(Collections.emptyList()));
        verify(session, times(1)).bind(
            eq(TableStatsService.UNNAMED),
            eq(TableStatsService.UNNAMED),
            eq(Collections.emptyList()),
            isNull(FormatCodes.FormatCode[].class));
        verify(session, times(1)).execute(
            eq(TableStatsService.UNNAMED),
            eq(0),
            any(TableStatsService.TableStatsResultReceiver.class));
        verify(session, times(1)).sync();
    }

    @Test
    public void testNoUpdateIfLocalNodeNotAvailable() throws Exception {
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(null);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, new HashSet<>(additionalClusterSettings())));
        final SQLOperations sqlOperations = mock(SQLOperations.class);

        TableStatsService statsService = new TableStatsService(
            Settings.EMPTY,
            THREAD_POOL,
            clusterService,
            () -> sqlOperations);

        statsService.run();
        Mockito.verify(sqlOperations, times(0)).createSession(anyString(), anySetOf(Option.class), anyByte());
    }
}
