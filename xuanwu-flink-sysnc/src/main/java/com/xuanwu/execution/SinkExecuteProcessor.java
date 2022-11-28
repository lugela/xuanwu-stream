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

package com.xuanwu.execution;

import com.google.common.collect.Lists;
import com.xuanwu.common.XuanwuContext;
import com.xuanwu.discovery.PluginIdentifier;
import com.xuanwu.discovery.SeaTunnelSinkPluginDiscovery;
import com.xuanwu.env.FlinkEnvironment;
import com.xuanwu.exception.TaskExecuteException;
import com.xuanwu.sink.XuanwuSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import scala.Serializable;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SinkExecuteProcessor extends AbstractPluginExecuteProcessor<XuanwuSink<String, Serializable, Serializable, Serializable>> {

    private static final String PLUGIN_TYPE = "sink";

    List<XuanwuSink<String, Serializable, Serializable, Serializable>> sinks;

    public SinkExecuteProcessor(FlinkEnvironment flinkEnvironment,
                                List<? extends Config> pluginConfigs) {
        super(flinkEnvironment, pluginConfigs);
    }

    @Override
    protected List<XuanwuSink<String, Serializable, Serializable, Serializable>> initializePlugins(List<? extends Config> pluginConfigs) {
        SeaTunnelSinkPluginDiscovery sinkPluginDiscovery = new SeaTunnelSinkPluginDiscovery(addUrlToClassloader);
        List<URL> pluginJars = new ArrayList<>();
        List<XuanwuSink<String, Serializable, Serializable, Serializable>> sinks = pluginConfigs.stream().map(sinkConfig -> {
            PluginIdentifier pluginIdentifier = PluginIdentifier.of(ENGINE_TYPE, PLUGIN_TYPE, sinkConfig.getString(PLUGIN_NAME));
            pluginJars.addAll(sinkPluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier)));
            XuanwuSink<String, Serializable, Serializable, Serializable> seaTunnelSink =
                sinkPluginDiscovery.createPluginInstance(pluginIdentifier);
            seaTunnelSink.prepare(sinkConfig);
            seaTunnelSink.setSeaTunnelContext(XuanwuContext.getContext());
            return seaTunnelSink;
        }).distinct().collect(Collectors.toList());
        flinkEnvironment.registerPlugin(pluginJars);
        this.sinks = sinks;
        return sinks;
    }

    @Override
    public List<DataStream<String>> execute(List<DataStream<String>> upstreamDataStreams) throws TaskExecuteException {
        DataStream<String> input = upstreamDataStreams.get(0);
        for (XuanwuSink<String, Serializable, Serializable, Serializable> sink:sinks){
            SinkFunction sinkFunction = sink.getSinkFunction();
            if (null != sinkFunction){
                input.addSink(sinkFunction);
            }
        }
        return null;
    }


}
