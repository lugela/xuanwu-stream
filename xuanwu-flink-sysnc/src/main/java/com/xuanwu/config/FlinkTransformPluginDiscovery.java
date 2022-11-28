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



import com.xuanwu.discovery.FlinkAbstractPluginDiscovery;
import com.xuanwu.discovery.PluginIdentifier;
import com.xuanwu.env.BaseFlinkTransform;


import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class FlinkTransformPluginDiscovery extends FlinkAbstractPluginDiscovery<BaseFlinkTransform> {

    public FlinkTransformPluginDiscovery() {
        super("flink");
    }

    @Override
    public List<URL> getPluginJarPaths(List<PluginIdentifier> pluginIdentifiers) {
        return new ArrayList<>();
    }

    @Override
    protected Class<BaseFlinkTransform> getPluginBaseClass() {
        return BaseFlinkTransform.class;
    }
}
