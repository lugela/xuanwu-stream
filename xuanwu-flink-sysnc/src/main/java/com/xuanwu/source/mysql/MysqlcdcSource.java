package com.xuanwu.source.mysql;

import com.google.auto.service.AutoService;
import com.xuanwu.common.XuanwuContext;
import com.xuanwu.source.XuanwuSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

/**
 * @description:
 * @author: lugela
 * @create: 2022-08-30 19:18
 */
@AutoService(XuanwuSource.class)
public class MysqlcdcSource implements XuanwuSource{

    XuanwuContext xuanwuContext;
    MySqlSource<String> build;

    @Override
    public String getPluginName() {
        return "MysqlcdcSource";
    }


    @Override
    public void prepare(Config pluginConfig) {
        //处理
        MysqlSourceOptions mysqlSourceOptions = MysqlSourceConfig.buildMysqlSourceOptions(pluginConfig);
        MySqlSource<String> build = mysqlSourceOptions.build();
        this.build = build;

    }

    @Override
    public void setXuanwuContext(XuanwuContext xuanwuContext) {
         this.xuanwuContext = xuanwuContext;
    }

    @Override
    public DataStream<String> getFlinkDataStreaingSource(){
       return this.xuanwuContext.getEnvironment().fromSource(this.build, WatermarkStrategy.noWatermarks(),"mysql cdc");
    }


}
