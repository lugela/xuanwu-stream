package com.xuanwu.sink.jdbc;

import com.google.auto.service.AutoService;
import com.xuanwu.common.XuanwuContext;
import com.xuanwu.exception.PrepareFailException;
import com.xuanwu.sink.XuanwuSink;
import com.xuanwu.sink.jdbc.config.JdbcParameters;
import com.xuanwu.sink.jdbc.config.JdbcSinkConfig;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @description:
 * @author: lugela
 * @create: 2022-09-01 16:07
 */

@AutoService(XuanwuSink.class)
public class JdbcSink implements XuanwuSink {
    XuanwuContext xuanwuContext;
    JdbcParameters jdbcParameters;
    TwoPhaseCommitSink twoPhaseCommitSink;
    Properties properties;

    @Override
    public String getPluginName() {
        return "jdbc";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
         this.properties = new Properties();
         JdbcParameters jdbcParameters = new JdbcParameters();

        //pluginConfig 处理musqlsink 端的参数
        if (pluginConfig.hasPath(JdbcSinkConfig.DRIVER_CLASS_NAME)){
            String driverClassName = pluginConfig.getString(JdbcSinkConfig.DRIVER_CLASS_NAME);
            properties.setProperty(JdbcSinkConfig.DRIVER_CLASS_NAME,driverClassName);
            jdbcParameters.setDriverClassName(driverClassName);
        }

        if (pluginConfig.hasPath(JdbcSinkConfig.PASSWORD)){
            String password = pluginConfig.getString(JdbcSinkConfig.PASSWORD);
            properties.setProperty(JdbcSinkConfig.PASSWORD,password);
            jdbcParameters.setPassword(password);
        }

        if (pluginConfig.hasPath(JdbcSinkConfig.JDBCURL)){
            String jdbcurl = pluginConfig.getString(JdbcSinkConfig.JDBCURL);
            properties.setProperty(JdbcSinkConfig.JDBCURL,jdbcurl);
            jdbcParameters.setJdbcUrl(jdbcurl);
        }

        if (pluginConfig.hasPath(JdbcSinkConfig.USERNAME)){
            String username = pluginConfig.getString(JdbcSinkConfig.USERNAME);
            properties.setProperty(JdbcSinkConfig.USERNAME,username);
            jdbcParameters.setUsername(username);
        }


        if (pluginConfig.hasPath(JdbcSinkConfig.SOURCE_TO_TARGET)){
            Config config = pluginConfig.getConfig(JdbcSinkConfig.SOURCE_TO_TARGET);
            Map<String, String> setSourc2targetTableNameMap = new HashMap<>();
            config.entrySet().stream().forEach(entry ->{
                setSourc2targetTableNameMap.put(entry.getKey(),config.getString(entry.getKey()));
            });
            jdbcParameters.setSourc2targetTableName(setSourc2targetTableNameMap);
        }

        if (pluginConfig.hasPath(JdbcSinkConfig.TARGET_TABLE_KEY)){
            Config config = pluginConfig.getConfig(JdbcSinkConfig.TARGET_TABLE_KEY);
            Map<String, String> targetTableKeyMap = new HashMap<>();
            config.entrySet().stream().forEach(entry ->{
                targetTableKeyMap.put(entry.getKey(),config.getString(entry.getKey()));
            });
            jdbcParameters.setTargetTableKey(targetTableKeyMap);
        }
        this.jdbcParameters = jdbcParameters;


    }
    @Override
    public void setSeaTunnelContext(XuanwuContext xuanwuContext) {
        this.xuanwuContext = xuanwuContext;
         this.twoPhaseCommitSink = new TwoPhaseCommitSink(jdbcParameters, properties);
    }

    @Override
    public SinkFunction getSinkFunction(){
       return twoPhaseCommitSink;
    }
}
