/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xuanwu.config;


import com.xuanwu.api.BaseSink;
import com.xuanwu.api.BaseSource;
import com.xuanwu.env.Execution;
import com.xuanwu.env.FlinkEnvironment;
import com.xuanwu.env.RuntimeEnv;
import com.xuanwu.stream.FlinkStreamExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to create {@link Execution}.
 *
 * @param <ENVIRONMENT> environment type
 */
public class ExecutionFactory<ENVIRONMENT extends RuntimeEnv> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionFactory.class);

    public AbstractExecutionContext<ENVIRONMENT> executionContext;

    public ExecutionFactory(AbstractExecutionContext<ENVIRONMENT> executionContext) {
        this.executionContext = executionContext;
    }

    public Execution<BaseSource<ENVIRONMENT>, BaseSink<ENVIRONMENT>, ENVIRONMENT> createExecution() {
        Execution execution = null;
        switch (executionContext.getEngine()) {
            case FLINK:
                FlinkEnvironment flinkEnvironment = (FlinkEnvironment) executionContext.getEnvironment();
                switch (executionContext.getJobMode()) {
                    case STREAMING:
                        execution = new FlinkStreamExecution(flinkEnvironment);
                        break;
                    default:
                        execution = new FlinkStreamExecution(flinkEnvironment);
                }
                break;
            default:
                throw new IllegalArgumentException("No suitable engine");
        }
        LOGGER.info("current execution is [{}]", execution.getClass().getName());
        return (Execution<BaseSource<ENVIRONMENT>, BaseSink<ENVIRONMENT>, ENVIRONMENT>) execution;
    }

}
