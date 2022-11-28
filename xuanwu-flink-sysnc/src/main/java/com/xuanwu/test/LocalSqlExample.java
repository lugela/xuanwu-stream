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

package com.xuanwu.test;


import com.xuanwu.submit.flink.sql.job.Executor;
import com.xuanwu.submit.flink.sql.job.JobInfo;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

public class LocalSqlExample {

    public static void main(String[] args) throws IOException, URISyntaxException {
        String configFile = getTestConfigFile("/examples/mysqlcdc2print.conf");
        String jobContent = FileUtils.readFileToString(new File(configFile), StandardCharsets.UTF_8);
        Executor.runJob(new JobInfo(jobContent));
    }

    public static String getTestConfigFile(String configFile) throws FileNotFoundException, URISyntaxException {
        URL resource = LocalSqlExample.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Could not find config: " + configFile);
        }
        return Paths.get(resource.toURI()).toString();
    }
}
