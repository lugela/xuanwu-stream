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

package com.xuanwu.source;



import com.xuanwu.common.PluginIdentifierInterface;
import com.xuanwu.common.XuanwuContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.Serializable;

/**

 * @param <T>      The type of records produced by the source.
 * @param <SplitT> The type of splits handled by the source.
 * @param <StateT> The type of checkpoint states.
 */
public interface XuanwuSource<T, SplitT extends SourceSplit, StateT extends Serializable>
    extends PluginIdentifierInterface, Serializable {


   default DataStream<String> getFlinkDataStreaingSource(){ return null;}


     void prepare(Config pluginConfig);

     void setXuanwuContext(XuanwuContext xuanwuContext);



}
