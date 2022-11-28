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


import com.xuanwu.env.FlinkEnvironment;
import com.xuanwu.utils.ReflectionUtils;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.function.BiConsumer;


public abstract class AbstractPluginExecuteProcessor<T> implements PluginExecuteProcessor {

    protected final FlinkEnvironment flinkEnvironment;
    protected final List<? extends Config> pluginConfigs;
    protected final List<T> plugins;
    protected static final String ENGINE_TYPE = "xuanwu";
    protected static final String PLUGIN_NAME = "plugin_name";

    protected final BiConsumer<ClassLoader, URL> addUrlToClassloader = (classLoader, url) -> {
        if (classLoader.getClass().getName().endsWith("SafetyNetWrapperClassLoader")) {
            URLClassLoader c = (URLClassLoader) ReflectionUtils.getField(classLoader, "inner").get();
            ReflectionUtils.invoke(c, "addURL", url);
        } else if (classLoader instanceof URLClassLoader) {
            ReflectionUtils.invoke(classLoader, "addURL", url);
        } else {
            throw new RuntimeException("Unsupported classloader: " + classLoader.getClass().getName());
        }
    };

    protected AbstractPluginExecuteProcessor(FlinkEnvironment flinkEnvironment,
                                             List<? extends Config> pluginConfigs) {
        this.flinkEnvironment = flinkEnvironment;
        this.pluginConfigs = pluginConfigs;
        this.plugins = initializePlugins(pluginConfigs);
    }

    protected abstract List<T> initializePlugins(List<? extends Config> pluginConfigs);


}
