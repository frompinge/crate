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

package io.crate.protocols.postgres;

import com.google.common.util.concurrent.ListenableFuture;
import io.crate.action.sql.ResultReceiver;
import io.crate.analyze.symbol.Field;
import io.crate.operation.collect.stats.StatsTables;
import io.crate.planner.Planner;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.List;

public interface Portal {

    String name();

    FormatCodes.FormatCode[] getLastResultFormatCodes();

    List<? extends DataType> getLastOutputTypes();

    String getLastQuery();

    Portal bind(String statementName, String query, Statement statement,
                List<Object> params, @Nullable FormatCodes.FormatCode[] resultFormatCodes);

    List<Field> describe();

    void execute(ResultReceiver resultReceiver, int maxRows);

    ListenableFuture<?> sync(Planner planner, StatsTables statsTables);

    void close();

    void finish();

    boolean finished();
}
