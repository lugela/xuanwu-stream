package com.xuanwu.json2sql;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @description:
 * @author: lugela
 * @create: 2022-09-04 9:42
 */

@Data
public class JdbcInsertData {
    private List<String > paramKeys;
    private List<Object > paramValues;
    private String writeDataSqlTemplate;
    private List<Map<String,String>> fieldNames;
}
