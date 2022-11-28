package com.xuanwu.json2sql;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * @description:
 * @author: lugela
 * @create: 2022-09-05 18:58
 */

public  class JdbcTemplate {

    public static JdbcInsertData writeDataSqlTemplate(JSONObject jsonObject, JSONArray mydataFieldInfos,String targetTableName,List<String> primaryKeys,JdbcSqlTemplate jdbcSqlTemplate) {

        Map<String, Object> params = JSONObject.parseObject(jsonObject.toJSONString(), new TypeReference<Map<String, Object>>() {
        });
        List<String > paramKeys = new ArrayList<>();
        List<Object > paramValues = new ArrayList<>();
        List<Map<String,String>>  fieldNames = new ArrayList<>();
        //存储问号
        List<String> values = new ArrayList<>();
        Map<String,Map<String,String>> fieldMap = new HashMap<>();
        for (Object object : mydataFieldInfos){
            Map<String,String> map = (Map<String,String>)object;
            String fieldName = map.get("fieldName");
            fieldMap.put(fieldName,map);
        }

        //分离出字段
        for (String key: params.keySet()) {
            Map<String, String> fieldNameMap = fieldMap.get(key);
            fieldNames.add(fieldNameMap);
            paramKeys.add(key);
            paramValues.add(params.get(key));
        }
        for (int i = 0 ; i < paramKeys.size() ; i++){
            values.add("?");
        }
        //反射实现接口
        String writeDataSqlTemplate = jdbcSqlTemplate.writeSqlTemplate(targetTableName,paramKeys, values,primaryKeys);
        JdbcInsertData jdbcInsertData = new JdbcInsertData();
        jdbcInsertData.setWriteDataSqlTemplate(writeDataSqlTemplate);
        jdbcInsertData.setParamKeys(paramKeys);
        jdbcInsertData.setParamValues(paramValues);
        jdbcInsertData.setFieldNames(fieldNames);
        return jdbcInsertData;


    }


    public static String getDeleteStatement(String tableName, List<String> primaryKeys) {
        String conditionClause =
                primaryKeys.stream()
                        .map(f -> format("%s = ?", f))
                        .collect(Collectors.joining(" AND "));
        return "DELETE FROM " + tableName + " WHERE " + conditionClause;
    }

    public static void main(String[] args) {
        List<String> p = new ArrayList<>();
        p.add("id");
        p.add("id1");
        String test = getDeleteStatement("test", p);
        System.out.println(test);

    }



}
