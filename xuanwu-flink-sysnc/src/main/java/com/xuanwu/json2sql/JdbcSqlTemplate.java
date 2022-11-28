package com.xuanwu.json2sql;

import java.util.List;

public interface JdbcSqlTemplate {

    String writeSqlTemplate(String tableName,List<String> columns,List<String> questionMarks,List<String> primaryKeys);

    boolean acceptsURL(String url);

}
