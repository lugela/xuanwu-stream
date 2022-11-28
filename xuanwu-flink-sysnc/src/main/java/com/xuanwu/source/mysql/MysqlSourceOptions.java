package com.xuanwu.source.mysql;

import com.xuanwu.source.MyJsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Properties;

/**
 * @description: jdbc config
 * @author: lugela
 * @create: 2022-08-28 14:48
 */


@Data
public class MysqlSourceOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    private int port = 3306;
    private String hostname;
    private String username;
    private String password;
    private String serverId;
    private String[] databaseList;
    private String[] tableList;
    private String serverTimeZone = ZoneId.systemDefault().getId();
    private StartupOptions startupOptions = StartupOptions.initial();
    private int splitSize;
    private int splitMetaGroupSize;
    private int fetchSize;
    private Duration connectTimeout;
    private int connectMaxRetries;
    private int connectionPoolSize;
    private double distributionFactorUpper;
    private double distributionFactorLower;
    private boolean includeSchemaChanges = false;
    private boolean scanNewlyAddedTableEnabled  = false;
    private Properties jdbcProperties;
    private Duration heartbeatInterval;
    private Properties dbzProperties;
    private String chunkKeyColumn;


    public MySqlSource<String> build(){

        MySqlSourceBuilder<String> builder = MySqlSource.<String>builder();
        if (StringUtils.isNotEmpty(hostname)){
            builder.hostname(hostname);
        }
        builder.port(port);

        if (StringUtils.isNotEmpty(username)){
            builder.username(username);
        }

        if (StringUtils.isNotEmpty(password)){
            builder.password(password);
        }

        if (StringUtils.isNotEmpty(serverId)){
            builder.serverId(serverId);
        }

        if (null != databaseList && databaseList.length>0){
            builder.databaseList(databaseList);
        }

        if (null != tableList && tableList.length>0){
            builder.tableList(tableList);
        }

        if (StringUtils.isNotEmpty(serverTimeZone)){
            builder.serverTimeZone(serverTimeZone);
        }

        if (null != startupOptions){
            builder.startupOptions(startupOptions);
        }

        if (splitSize > 0){
            builder.splitSize(splitSize);
        }

        if (splitMetaGroupSize > 0){
            builder.splitMetaGroupSize(splitMetaGroupSize);
        }


        if (fetchSize > 0){
            builder.fetchSize(fetchSize);
        }

        if (null != connectTimeout){
            builder.connectTimeout(connectTimeout);
        }


        if (connectMaxRetries > 0){
            builder.connectMaxRetries(connectMaxRetries);
        }


        if (connectionPoolSize > 0){
            builder.connectionPoolSize(connectionPoolSize);
        }


        if (distributionFactorUpper > 0){
            builder.distributionFactorUpper(distributionFactorUpper);
        }


        if (distributionFactorLower > 0){
            builder.distributionFactorLower(distributionFactorLower);
        }


        if (distributionFactorLower > 0){
            builder.distributionFactorLower(distributionFactorLower);
        }

        builder.includeSchemaChanges(includeSchemaChanges);
        builder.scanNewlyAddedTableEnabled(scanNewlyAddedTableEnabled);

        if (null != jdbcProperties){
            builder.jdbcProperties(jdbcProperties);
        }

        if (null != heartbeatInterval){
            builder.heartbeatInterval(heartbeatInterval);
        }

        if (null != dbzProperties){
            dbzProperties.put("decimal.handling.mode", "string");
            builder.debeziumProperties(dbzProperties);
        }else{
            Properties properties = new Properties();
            properties.put("decimal.handling.mode", "string");
            builder.debeziumProperties(properties);
        }

        if (StringUtils.isNotEmpty(chunkKeyColumn)){
            builder.chunkKeyColumn(chunkKeyColumn);
        }

        builder.deserializer(new MyJsonDebeziumDeserializationSchema());

        return builder.build();
    }

}
