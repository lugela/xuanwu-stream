package com.xuanwu.source.oracle;


import com.xuanwu.source.MyJsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.connectors.oracle.table.StartupOptions;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.io.Serializable;
import java.time.ZoneId;
import java.util.Properties;

/**
 * @description: jdbc config
 * @author: lugela
 * @create: 2022-08-28 14:48
 */


@Data
public class OracleSourceOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    private int port = 1521;
    private String url;
    private String hostname;
    private String username;
    private String password;
    private String[] schemaList;
    private String database;
    private String[] tableList;
    private String serverTimeZone = ZoneId.systemDefault().getId();
    private StartupOptions startupOptions = StartupOptions.initial();
    private Properties dbzProperties;

    public SourceFunction<String> build(){

        OracleSource.Builder<String> builder = OracleSource.<String>builder();

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


        if (StringUtils.isNotEmpty(database)){
            builder.database(database);
        }


        if (StringUtils.isNotEmpty(url)){
            builder.url(url);
        }


        if (null != tableList && tableList.length>0){
            builder.tableList(tableList);
        }

        if (null != schemaList && schemaList.length>0){
            builder.schemaList(schemaList);
        }

        if (null != dbzProperties){
            dbzProperties.put("decimal.handling.mode", "string");
            builder.debeziumProperties(dbzProperties);
        }else{
            Properties properties = new Properties();
            properties.put("decimal.handling.mode", "string");
            builder.debeziumProperties(properties);
        }

        if (null != startupOptions){
            builder.startupOptions(startupOptions);
        }

        builder.deserializer(new MyJsonDebeziumDeserializationSchema());

        return builder.build();
    }

}
