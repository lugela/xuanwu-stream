/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xuanwu.submit.flink.sql;


import com.xuanwu.args.FlinkCommandArgs;
import com.xuanwu.config.FlinkJobType;
import com.xuanwu.submit.flink.sql.job.Executor;
import com.xuanwu.submit.flink.sql.job.JobInfo;
import com.xuanwu.utils.CommandLineUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class XuanwuSql {

    public static void main(String[] args) throws Exception {
        JobInfo jobInfo = parseJob(args);
        Executor.runJob(jobInfo);
    }

    private static JobInfo parseJob(String[] args) throws IOException {
        FlinkCommandArgs flinkArgs = CommandLineUtils.parse(args, new FlinkCommandArgs(), FlinkJobType.SQL.getType(), true);
        String configFilePath = flinkArgs.getConfigFile();
        String jobContent = FileUtils.readFileToString(new File(configFilePath), StandardCharsets.UTF_8);
        JobInfo jobInfo = new JobInfo(jobContent);
        jobInfo.substitute(flinkArgs.getVariables());
        return jobInfo;
    }

}
