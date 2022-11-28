package com.xuanwu.sink.jdbc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.xuanwu.json2sql.JdbcDialectLoader;
import com.xuanwu.json2sql.JdbcInsertData;
import com.xuanwu.json2sql.JdbcSqlTemplate;
import com.xuanwu.json2sql.JdbcTemplate;
import com.xuanwu.sink.jdbc.config.JdbcParameters;
import com.xuanwu.utils.HikariUtil;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @description:
 * @author: lugela
 * @create: 2022-09-01 13:48
 */

public class TwoPhaseCommitSink extends TwoPhaseCommitSinkFunction<String, HikariUtil,Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwoPhaseCommitSink.class);

    private JdbcParameters jdbcParameters;

    private transient AtomicLong numPendingRequests;
    private final long bufferFlushMaxMutations = 1000L;
    private final long bufferFlushIntervalMillis = 1000L;
    private transient ScheduledExecutorService executor;
    private transient ScheduledFuture scheduledFuture;
    private  Properties properties;
    private JdbcSqlTemplate jdbcSqlTemplate;
    private List<JdbcInsertData> cacheDataList;

    public TwoPhaseCommitSink(JdbcParameters jdbcParameters, Properties properties) {
        super(new KryoSerializer<>(HikariUtil.class,new ExecutionConfig()), VoidSerializer.INSTANCE);
        this.jdbcParameters = jdbcParameters;
        this.properties = properties;
        this.jdbcSqlTemplate = JdbcDialectLoader.load(jdbcParameters.getJdbcUrl());
        this.cacheDataList = new ArrayList<>();
    }

    /**
     * 执行数据库入库操作  task初始化的时候调用
     * @param hikariUtil
     * @param message
     * @param context
     * @throws Exception
     */

    @Override
    protected void invoke(HikariUtil hikariUtil, String message, Context context) throws Exception {
        //LOGGER.info(Thread.currentThread().getId()+":start invoke...");
        //LOGGER.info(message);
        JSONObject jsonObject = JSON.parseObject(message);
        //获取数据
        JSONObject mydataValue = jsonObject.getJSONObject("mydataValue");
        JSONArray mydataFieldInfos = jsonObject.getJSONArray("mydataFieldInfos");
        //获取表名
        JSONObject soruceJson = mydataValue.getJSONObject("source");
        String soruceTableName = soruceJson.getString("table");
        Map<String, String> sourc2targetTableName = jdbcParameters.getSourc2targetTableName();
        //目标表table的主键指定
        Map<String, String> targetTableKey = jdbcParameters.getTargetTableKey();
        //目标表
        String targetTableName = sourc2targetTableName.get(soruceTableName);

        List<String> primaryKeyList = null;
        if (null != targetTableKey && targetTableKey.containsKey(targetTableName)){
            String primaryKeys = targetTableKey.get(targetTableName);
            primaryKeyList = Arrays.stream(primaryKeys.split(",")).collect(Collectors.toList());

        }

        //判断op 类型
        if (mydataValue.containsKey("op")){
            String op = mydataValue.getString("op");
            //删除
            if ("d".equals(op)){
                //先提交未提交的内容
                handleData(hikariUtil);

                Map<String,Map<String,String>> fieldMap = new HashMap<>();
                for (Object object : mydataFieldInfos){
                    Map<String,String> map = (Map<String,String>)object;
                    String fieldName = map.get("fieldName");
                    fieldMap.put(fieldName,map);
                }
                
                //获取字段并且拼接字段
                JSONObject beforeJson = mydataValue.getJSONObject("before");
                String deleteStatement = JdbcTemplate.getDeleteStatement(targetTableName, primaryKeyList);
                //然后进行删除操作
                Connection conn = hikariUtil.getconn();
                PreparedStatement ps = conn.prepareStatement(deleteStatement);
                for (int i =0;i<primaryKeyList.size();i++){
                    String key = primaryKeyList.get(i);
                    Object value = beforeJson.get(key.toLowerCase());
                    Map<String, String> stringStringMap = fieldMap.get(key);
                    setPsStatement(value, ps, i+1,stringStringMap);
                }
                ps.execute();
                hikariUtil.commit();
                ps.close();
                //应该先进行commit 操作,而后进行delete
            }else if ("r".equals(op) || "i".equals(op) || "u".equals(op) || "c".equals(op) ){
                //进行插入操作
                JSONObject afterJson = mydataValue.getJSONObject("after");
                JdbcInsertData jdbcInsertData = JdbcTemplate.writeDataSqlTemplate(afterJson,mydataFieldInfos,targetTableName,primaryKeyList,jdbcSqlTemplate);
                cacheDataList.add(jdbcInsertData);

                if (this.bufferFlushMaxMutations > 0L && this.numPendingRequests.incrementAndGet() >= this.bufferFlushMaxMutations) {
                    handleData(hikariUtil);
                }
            }

        }

    }

    @Override
    protected HikariUtil beginTransaction() throws Exception {
        this.numPendingRequests = new AtomicLong(0L);
        return new HikariUtil(properties);
    }

    @Override
    protected void preCommit(HikariUtil hikariUtil) throws Exception {
    }

    @Override
    protected void commit(HikariUtil hikariUtil) {
        if(cacheDataList.size()>0){
            LOGGER.info("start commit...");
            try {
                handleData(hikariUtil);
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }



    }

    @Override
    protected void abort(HikariUtil hikariUtil) {
        LOGGER.info("start abort rollback...");
        hikariUtil.rollback();
    }

    @Override
    public void writeWatermark(Watermark watermark) throws Exception {
        super.writeWatermark(watermark);
    }

    private void handleData(HikariUtil hikariUtil) throws SQLException {
        //获取表对应关系
        Connection conn = hikariUtil.getconn();
        for (JdbcInsertData jdbcInsertData: cacheDataList){
            List<Object> paramValues = jdbcInsertData.getParamValues();
            String writeDataSqlTemplate = jdbcInsertData.getWriteDataSqlTemplate();
            List<Map<String,String>> fieldNames = jdbcInsertData.getFieldNames();
            PreparedStatement ps = conn.prepareStatement(writeDataSqlTemplate);
            for (int i = 0 ; i <paramValues.size();i++ ){
                Map<String, String> fieldMap = fieldNames.get(i);
                Object o = paramValues.get(i);
                setPsStatement(o,ps,i+1,fieldMap);
            }
            ps.execute();
            ps.close();
        }
        cacheDataList.clear();
        LOGGER.info("start invoke commit...");
        hikariUtil.commit();
        this.numPendingRequests = new AtomicLong(0L);
    }

    private void setPsStatement(Object value,PreparedStatement ps,int parameterIndex,Map<String,String> fieldMap) throws SQLException {
        if (null == value){
            ps.setObject(parameterIndex,null);
        }
        if(value instanceof String){
            ps.setString(parameterIndex,(String) value);
        }else if (value instanceof Long){
            String fieldType = fieldMap.get("fieldType");
            String fieldNameSchema = fieldMap.get("fieldNameSchema");
            //格式化时间
            if ("INT64".equals(fieldType) && "io.debezium.time.Timestamp".equals(fieldNameSchema)){
                //
                LocalDateTime longToLocalDateTime =
                        LocalDateTime.ofInstant(Instant.ofEpochMilli((Long)value), ZoneId.systemDefault());
                ps.setTimestamp(parameterIndex, Timestamp.valueOf(longToLocalDateTime));
            }else {
                ps.setLong(parameterIndex,(Long) value);
            }

        }else if (value instanceof BigDecimal){
            ps.setBigDecimal(parameterIndex,(BigDecimal) value);
        }else if (value instanceof Integer){
            ps.setInt(parameterIndex,(Integer) value);
        }
        else {
            ps.setObject(parameterIndex,value);
        }
    }
}

