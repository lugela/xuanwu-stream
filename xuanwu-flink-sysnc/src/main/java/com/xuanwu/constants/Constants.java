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

package com.xuanwu.constants;

public final class Constants {
    public static final String ROW_ROOT = "__root__";
    public static final String ROW_TMP = "__tmp__";

    public static final String LOGO = "SeaTunnel";

    public static final String SOURCE = "source";

    public static final String TRANSFORM = "transform";

    public static final String SINK = "sink";

    public static final String SOURCE_SERIALIZATION = "source.serialization";

    public static final String SOURCE_PARALLELISM = "source.parallelism";

    public static final String HDFS_ROOT = "hdfs.root";

    public static final String HDFS_USER = "hdfs.user";

    public static final String CHECKPOINT_INTERVAL = "checkpoint.interval";

    public static final String CHECKPOINT_ID = "checkpoint.id";

    public static final String UUID = "uuid";

    public static final String NOW = "now";

    private Constants() {
    }


    public static final int USAGE_EXIT_CODE = 234;
}
