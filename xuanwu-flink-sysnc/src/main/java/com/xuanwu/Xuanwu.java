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

package com.xuanwu;


import com.xuanwu.command.Command;
import com.xuanwu.command.CommandArgs;
import com.xuanwu.exception.CommandException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Xuanwu {
    private static final Logger LOGGER = LoggerFactory.getLogger(Xuanwu.class);

    /**
     * This method is the entrypoint of SeaTunnel.
     *
     * @param command commandArgs
     * @param <T>         commandType
     */
    public static <T extends CommandArgs> void run(Command<T> command) throws CommandException {
        try {
            command.execute();
        } catch (Exception e) {
            showFatalError(e);
        }
    }

    private static void showConfigError(Throwable throwable) {
        LOGGER.error(
            "\n\n===============================================================================\n\n");
        String errorMsg = throwable.getMessage();
        LOGGER.error("Config Error:\n");
        LOGGER.error("Reason: {} \n", errorMsg);
        LOGGER.error(
            "\n===============================================================================\n\n\n");
    }

    private static void showFatalError(Throwable throwable) {
        LOGGER.error(
            "\n\n===============================================================================\n\n");
        String errorMsg = throwable.getMessage();
        LOGGER.error("Fatal Error, \n");
        // FIX

        LOGGER.error("Reason:{} \n", errorMsg);
        LOGGER.error("Exception StackTrace:{} ", ExceptionUtils.getStackTrace(throwable));
        LOGGER.error(
            "\n===============================================================================\n\n\n");
    }
}
