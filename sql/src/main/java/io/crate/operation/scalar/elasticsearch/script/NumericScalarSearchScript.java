/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.scalar.elasticsearch.script;

import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.operation.operator.Operator;
import io.crate.planner.symbol.Literal;
import io.crate.types.DataType;
import io.crate.types.DoubleType;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptModule;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class NumericScalarSearchScript extends NumericScalarSortScript {

    public static final String NAME = "numeric_scalar_search";

    public static void register(ScriptModule module) {
        module.registerScript(NAME, Factory.class);
    }

    public static class Factory extends AbstractScalarSearchScriptFactory {

        protected String scalarName;

        @Inject
        public Factory(Functions functions) {
            super(functions);
        }

        /**
         * This method is called for every search on every shard.
         *
         * @param params list of script parameters passed with the query
         * @return new native script
         */
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            scalarName = params == null ? null : XContentMapValues.nodeStringValue(params.get("scalar_name"), null);
            if (scalarName == null) {
                throw new ScriptException("Missing the scalar_name parameter");
            }

            prepare(params);

            return new NumericScalarSearchScript(fieldName, fieldType,
                    function, arguments, valueLiteral, operator);
        }

        @Override
        protected String functionName() {
            return scalarName;
        }
    }

    private final Operator operator;
    private final Literal value;

    public NumericScalarSearchScript(String fieldName, DataType fieldType,
                                     Scalar function, @Nullable List<Input> arguments,
                                     Literal value, Operator operator) {
        super(fieldName, fieldType, function, arguments, null);
        this.value = value;
        this.operator = operator;
    }

    @Override
    public Object run() {
        ScriptDocValues docValue = (ScriptDocValues) doc().get(fieldName);

        if (docValue != null && !docValue.isEmpty()) {
            Object functionReturn = evaluateScalar(docValue);
            Literal left = Literal.newLiteral(DoubleType.INSTANCE.value(functionReturn));
            return operator.evaluate(left, value);
        }
        return false;
    }

}
