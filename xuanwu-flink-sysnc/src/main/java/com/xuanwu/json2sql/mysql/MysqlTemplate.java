package com.xuanwu.json2sql.mysql;

import com.google.auto.service.AutoService;
import com.xuanwu.json2sql.JdbcSqlTemplate;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.List;

/**
 * @description:
 * @author: lugela
 * @create: 2022-09-05 23:19
 */

@AutoService(JdbcSqlTemplate.class)
public class MysqlTemplate implements JdbcSqlTemplate, Serializable {

    @Override
    public String writeSqlTemplate(String tableName,List<String> columns, List<String> questionMarks,List<String> primaryKeys) {
        String writeDataSqlTemplate = new StringBuilder()
                .append("INSERT INTO ").append(tableName).append("(").append(StringUtils.join(columns, ","))
                .append(") VALUES(").append(StringUtils.join(questionMarks, ","))
                .append(")")
                .append(onDuplicateKeyUpdateString(columns))
                .toString();

        return writeDataSqlTemplate;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:mysql:");
    }

    private  String onDuplicateKeyUpdateString(List<String> columnHolders) {
        if (columnHolders == null || columnHolders.size() < 1) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(" ON DUPLICATE KEY UPDATE ");
        boolean first = true;
        for (String column : columnHolders) {
            if (!first) {
                sb.append(",");
            } else {
                first = false;
            }
            sb.append(column);
            sb.append("=VALUES(");
            sb.append(column);
            sb.append(")");
        }
        return sb.toString();
    }


}
