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
import com.xuanwu.api.BaseTransform;
import com.xuanwu.constants.EngineType;
import com.xuanwu.constants.PluginType;
import com.xuanwu.discovery.FlinkSinkPluginDiscovery;
import com.xuanwu.discovery.FlinkSourcePluginDiscovery;
import com.xuanwu.discovery.PluginIdentifier;
import com.xuanwu.env.FlinkEnvironment;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class FlinkExecutionContext extends AbstractExecutionContext<FlinkEnvironment> {
    private final FlinkSourcePluginDiscovery flinkSourcePluginDiscovery;
    private final FlinkTransformPluginDiscovery flinkTransformPluginDiscovery;
    private final FlinkSinkPluginDiscovery flinkSinkPluginDiscovery;
    private final List<URL> pluginJars;

    public FlinkExecutionContext(Config config, EngineType engine) {
        super(config, engine);
        this.flinkSourcePluginDiscovery = new FlinkSourcePluginDiscovery();
        this.flinkTransformPluginDiscovery = new FlinkTransformPluginDiscovery();
        this.flinkSinkPluginDiscovery = new FlinkSinkPluginDiscovery();
        Set<URL> pluginJars = new HashSet<>();
        // since we didn't split the transform plugin jars, we just need to register the source/sink plugin jars
        pluginJars.addAll(flinkSourcePluginDiscovery.getPluginJarPaths(getPluginIdentifiers(PluginType.SOURCE)));
        pluginJars.addAll(flinkSinkPluginDiscovery.getPluginJarPaths(getPluginIdentifiers(PluginType.SINK)));
        this.pluginJars = new ArrayList<>(pluginJars);
        this.getEnvironment().registerPlugin(this.pluginJars);
    }

    @Override
    public List<BaseSource<FlinkEnvironment>> getSources() {
        final String pluginType = PluginType.SOURCE.getType();
        final String engineType = EngineType.FLINK.getEngine();
        final List<? extends Config> configList = getRootConfig().getConfigList(pluginType);
        return configList.stream()
            .map(pluginConfig -> {
                PluginIdentifier pluginIdentifier = PluginIdentifier.of(engineType, pluginType, pluginConfig.getString("plugin_name"));
                BaseSource<FlinkEnvironment> pluginInstance = flinkSourcePluginDiscovery.createPluginInstance(pluginIdentifier);
                pluginInstance.setConfig(pluginConfig);
                return pluginInstance;
            }).collect(Collectors.toList());
    }

    @Override
    public List<BaseTransform<FlinkEnvironment>> getTransforms() {
        final String pluginType = PluginType.TRANSFORM.getType();
        final String engineType = EngineType.FLINK.getEngine();
        final List<? extends Config> configList = getRootConfig().getConfigList(pluginType);
        return configList.stream()
            .map(pluginConfig -> {
                PluginIdentifier pluginIdentifier = PluginIdentifier.of(engineType, pluginType, pluginConfig.getString("plugin_name"));
                BaseTransform<FlinkEnvironment> pluginInstance = flinkTransformPluginDiscovery.createPluginInstance(pluginIdentifier);
                pluginInstance.setConfig(pluginConfig);
                return pluginInstance;
            }).collect(Collectors.toList());
    }

    @Override
    public List<BaseSink<FlinkEnvironment>> getSinks() {
        final String pluginType = PluginType.SINK.getType();
        final String engineType = EngineType.FLINK.getEngine();
        final List<? extends Config> configList = getRootConfig().getConfigList(pluginType);
        return configList.stream()
            .map(pluginConfig -> {
                PluginIdentifier pluginIdentifier = PluginIdentifier.of(engineType, pluginType, pluginConfig.getString("plugin_name"));
                BaseSink<FlinkEnvironment> pluginInstance = flinkSinkPluginDiscovery.createPluginInstance(pluginIdentifier);
                pluginInstance.setConfig(pluginConfig);
                return pluginInstance;
            }).collect(Collectors.toList());
    }

    @Override
    public List<URL> getPluginJars() {
        return pluginJars;
    }
}
