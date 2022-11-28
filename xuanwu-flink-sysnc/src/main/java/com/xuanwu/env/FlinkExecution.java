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

package com.xuanwu.env;

import com.xuanwu.common.XuanwuContext;
import com.xuanwu.constants.EngineType;
import com.xuanwu.exception.TaskExecuteException;
import com.xuanwu.execution.PluginExecuteProcessor;
import com.xuanwu.execution.SinkExecuteProcessor;
import com.xuanwu.execution.SourceExecuteProcessor;
import com.xuanwu.execution.TaskExecution;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Used to execute a SeaTunnelTask.
 */
public class FlinkExecution implements TaskExecution {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkExecution.class);

    private final Config config;
    private final FlinkEnvironment flinkEnvironment;
    private PluginExecuteProcessor sourcePluginExecuteProcessor;
    private PluginExecuteProcessor sinkPluginExecuteProcessor;
    public FlinkExecution(Config config) {
        this.config = config;
        this.flinkEnvironment = new EnvironmentFactory<FlinkEnvironment>(config, EngineType.FLINK).getEnvironment();
        XuanwuContext.getContext().setJobMode(flinkEnvironment.getJobMode());
        XuanwuContext.getContext().setEnvironment(this.flinkEnvironment.getStreamExecutionEnvironment());
        this.sourcePluginExecuteProcessor = new SourceExecuteProcessor(flinkEnvironment, config.getConfigList("source"));
        this.sinkPluginExecuteProcessor = new SinkExecuteProcessor(flinkEnvironment, config.getConfigList("sink"));


    }

    @Override
    public void execute() throws TaskExecuteException {
        List<DataStream<String>> dataStreams = new ArrayList<>();
        sourcePluginExecuteProcessor.execute(dataStreams);
        sinkPluginExecuteProcessor.execute(dataStreams);
        LOGGER.info("Flink Execution Plan:{}", flinkEnvironment.getStreamExecutionEnvironment().getExecutionPlan());
        try {
            flinkEnvironment.getStreamExecutionEnvironment().execute(flinkEnvironment.getJobName());
        } catch (Exception e) {
            throw new TaskExecuteException("Execute Flink job error", e);
        }
    }
}
