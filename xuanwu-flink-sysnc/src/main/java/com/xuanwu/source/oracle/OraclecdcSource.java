package com.xuanwu.source.oracle;

import com.google.auto.service.AutoService;
import com.xuanwu.common.XuanwuContext;
import com.xuanwu.source.XuanwuSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

/**
 * @description:
 * @author: lugela
 * @create: 2022-08-30 19:18
 */
@AutoService(XuanwuSource.class)
public class OraclecdcSource implements XuanwuSource{

    XuanwuContext xuanwuContext;
    SourceFunction<String> build;

    @Override
    public String getPluginName() {
        return "OraclecdcSource";
    }


    @Override
    public void prepare(Config pluginConfig) {
        //处理
        OracleSourceOptions oracleSourceOptions = OracleSourceConfig.buildOracleSourceOptions(pluginConfig);
        this.build = oracleSourceOptions.build();
    }

    @Override
    public void setXuanwuContext(XuanwuContext xuanwuContext) {
         this.xuanwuContext = xuanwuContext;
    }

    @Override
    public DataStream<String> getFlinkDataStreaingSource(){
       return this.xuanwuContext.getEnvironment().addSource(this.build,"oracle cdc");
    }


}
