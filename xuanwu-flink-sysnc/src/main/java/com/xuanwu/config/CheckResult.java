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

import lombok.Data;

@Data
public class CheckResult {

    private static final CheckResult SUCCESS = new CheckResult(true, "");

    private boolean success;

    private String msg;

    /**
     * Do not call this constructor directly,
     * please use {@link #success} or {@link #error(String)} instead,
     * will be private in the future
     */
    @Deprecated
    public CheckResult(boolean success, String msg) {
        this.success = success;
        this.msg = msg;
    }

    /**
     * @return a successful instance of CheckResult
     */
    public static CheckResult success() {
        return SUCCESS;
    }

    /**
     * @param msg the error message
     * @return an error instance of CheckResult
     */
    public static CheckResult error(String msg) {
        return new CheckResult(false, msg);
    }

}
