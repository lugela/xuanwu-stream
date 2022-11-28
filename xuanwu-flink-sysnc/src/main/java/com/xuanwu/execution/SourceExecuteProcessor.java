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
import com.xuanwu.discovery.SeaTunnelSourcePluginDiscovery;
import com.xuanwu.env.FlinkEnvironment;
import com.xuanwu.source.XuanwuSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class SourceExecuteProcessor extends AbstractPluginExecuteProcessor<XuanwuSource> {

    private static final String PLUGIN_TYPE = "source";

    public SourceExecuteProcessor(FlinkEnvironment flinkEnvironment,
                                  List<? extends Config> sourceConfigs) {
        super(flinkEnvironment, sourceConfigs);
    }

    @Override
    public List<DataStream<String>> execute(List<DataStream<String>> upstreamDataStreams) {
        for (int i = 0; i < plugins.size(); i++) {
            XuanwuSource internalSource = plugins.get(i);
            DataStream<String> flinkDataStreaingSource = internalSource.getFlinkDataStreaingSource();
            upstreamDataStreams.add(flinkDataStreaingSource);
        }

        return null;
    }

    @Override
    protected List<XuanwuSource> initializePlugins(List<? extends Config> pluginConfigs) {
        SeaTunnelSourcePluginDiscovery sourcePluginDiscovery = new SeaTunnelSourcePluginDiscovery(addUrlToClassloader);
        List<XuanwuSource> sources = new ArrayList<>();
        Set<URL> jars = new HashSet<>();
        for (Config sourceConfig : pluginConfigs) {
            PluginIdentifier pluginIdentifier = PluginIdentifier.of(
                ENGINE_TYPE, PLUGIN_TYPE, sourceConfig.getString(PLUGIN_NAME));
            jars.addAll(sourcePluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier)));
            XuanwuSource xuanwuSource = sourcePluginDiscovery.createPluginInstance(pluginIdentifier);
            xuanwuSource.prepare(sourceConfig);
            xuanwuSource.setXuanwuContext(XuanwuContext.getContext());
            sources.add(xuanwuSource);
        }
        flinkEnvironment.registerPlugin(new ArrayList<>(jars));
        return sources;
    }
}
