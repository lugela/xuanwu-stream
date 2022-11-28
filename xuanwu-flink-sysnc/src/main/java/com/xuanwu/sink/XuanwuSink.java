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

package com.xuanwu.sink;

import com.xuanwu.common.PluginIdentifierInterface;
import com.xuanwu.serialization.Serializer;
import com.xuanwu.source.SeaTunnelContextAware;
import com.xuanwu.table.type.SeaTunnelDataType;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;


public interface XuanwuSink<IN, StateT, CommitInfoT, AggregatedCommitInfoT>
    extends Serializable, PluginIdentifierInterface, XuanwyPluginLifeCycle, SeaTunnelContextAware {

    /**
     * Set the row type info of sink row data. This method will be automatically called by translation.
     *
     * @param seaTunnelRowType The row type info of sink.
     */
    default void setTypeInfo(SeaTunnelDataType<String> seaTunnelRowType) {

    }

    default SinkFunction getSinkFunction(){
        return null;
    }

    /**
     * Get the data type of the records consumed by this sink.
     *
     * @return SeaTunnel data type.
     */
    default SeaTunnelDataType<IN> getConsumedType(){
        return null;
    }

    /**
     * This method will be called to creat {@link SinkWriter}
     *
     * @param context The sink context
     * @return Return sink writer instance
     * @throws IOException throws IOException when createWriter failed.
     */
    default SinkWriter<IN, CommitInfoT, StateT> createWriter(SinkWriter.Context context) throws IOException{
        return null;
    }

    default SinkWriter<IN, CommitInfoT, StateT> restoreWriter(SinkWriter.Context context,
                                                              List<StateT> states) throws IOException {
        return createWriter(context);
    }

    /**
     * Get {@link StateT} serializer. So that {@link StateT} can be transferred across processes
     *
     * @return Serializer of {@link StateT}
     */
    default Optional<Serializer<StateT>> getWriterStateSerializer() {
        return Optional.empty();
    }

    /**
     * This method will be called to create {@link SinkCommitter}
     *
     * @return Return sink committer instance
     * @throws IOException throws IOException when createCommitter failed.
     */
    default Optional<SinkCommitter<CommitInfoT>> createCommitter() throws IOException {
        return Optional.empty();
    }

    /**
     * Get {@link CommitInfoT} serializer. So that {@link CommitInfoT} can be transferred across processes
     *
     * @return Serializer of {@link CommitInfoT}
     */
    default Optional<Serializer<CommitInfoT>> getCommitInfoSerializer() {
        return Optional.empty();
    }


    default Optional<SinkAggregatedCommitter<CommitInfoT, AggregatedCommitInfoT>> createAggregatedCommitter() throws IOException {
        return Optional.empty();
    }

    /**
     * Get {@link AggregatedCommitInfoT} serializer. So that {@link AggregatedCommitInfoT} can be transferred across processes
     *
     * @return Serializer of {@link AggregatedCommitInfoT}
     */
    default Optional<Serializer<AggregatedCommitInfoT>> getAggregatedCommitInfoSerializer() {
        return Optional.empty();
    }

}
