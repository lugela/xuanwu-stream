package com.xuanwu.json2sql.oracle;

import com.google.auto.service.AutoService;
import com.xuanwu.json2sql.JdbcSqlTemplate;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @description:
 * @author: lugela
 * @create: 2022-09-05 23:19
 */

@AutoService(JdbcSqlTemplate.class)
public class OracleTemplate implements JdbcSqlTemplate, Serializable {

    @Override
    public String writeSqlTemplate(String tableName,List<String> columns, List<String> questionMarks,List<String> primaryKeys) {
        return getUpsertStatement(tableName,columns,primaryKeys,true);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:oracle:thin:");
    }

    //oracle 拼接写法
    public static String  getUpsertStatement(String tableName, List<String> fieldNames, List<String> uniqueKeyFields, boolean allReplace) {
        StringBuilder mergeIntoSql = new StringBuilder();
        mergeIntoSql.append("MERGE INTO " + tableName + " T1 USING (").append(buildDualQueryStatement(fieldNames)).append(") T2 ON (").append(buildConnectionConditions(uniqueKeyFields) + ") ");
        String updateSql = buildUpdateConnection(fieldNames, uniqueKeyFields, allReplace);
        if (StringUtils.isNotEmpty(updateSql)) {
            mergeIntoSql.append(" WHEN MATCHED THEN UPDATE SET ");
            mergeIntoSql.append(updateSql);
        }

        mergeIntoSql.append(" WHEN NOT MATCHED THEN ").append("INSERT (").append(fieldNames.stream().map((col) -> {
            return quoteIdentifier(col);
        }).collect(Collectors.joining(","))).append(") VALUES (").append(fieldNames.stream().map((col) -> {
            return "T2." + quoteIdentifier(col);
        }).collect(Collectors.joining(","))).append(")");
        return parseNamedStatement(mergeIntoSql.toString(), new HashMap<>());
    }


    private static  String parseNamedStatement(String sql, Map<String, List<Integer>> paramMap) {
        StringBuilder parsedSql = new StringBuilder();
        int fieldIndex = 1;
        int length = sql.length();

        for(int i = 0; i < length; ++i) {
            char c = sql.charAt(i);
            if (':' != c) {
                parsedSql.append(c);
            } else {
                int j;
                for(j = i + 1; j < length && Character.isJavaIdentifierPart(sql.charAt(j)); ++j) {
                }

                String parameterName = sql.substring(i + 1, j);
                ((List)paramMap.computeIfAbsent(parameterName, (n) -> {
                    return new ArrayList();
                })).add(fieldIndex);
                ++fieldIndex;
                i = j - 1;
                parsedSql.append('?');
            }
        }
        return parsedSql.toString();
    }




    private static String buildDualQueryStatement(List<String> column) {
        StringBuilder sb = new StringBuilder("SELECT ");
        String collect = column.stream().map((col) -> {
            return wrapperPlaceholder(col) + quoteIdentifier(col);
        }).collect(Collectors.joining(", "));
        sb.append(collect).append(" FROM DUAL");
        return sb.toString();
    }


    private static String buildUpdateConnection(List<String> fieldNames, List<String> uniqueKeyFields, boolean allReplace) {
        String updateConnectionSql = fieldNames.stream().filter((col) -> {
            boolean bbool = !uniqueKeyFields.contains(col.toLowerCase()) && !uniqueKeyFields.contains(col.toUpperCase());
            return bbool;
        }).map((col) -> {
            return buildConnectionByAllReplace(allReplace, col);
        }).collect(Collectors.joining(","));
        return updateConnectionSql;
    }


    private static String quoteIdentifier(String identifier) {
        return "" + identifier + "";
    }

    private static String wrapperPlaceholder(String fieldName) {
        return " :" + fieldName + " ";
    }


    private static String buildConnectionByAllReplace(boolean allReplace, String col) {
        String conncetionSql = allReplace ? quoteIdentifier("T1") + "." + quoteIdentifier(col) + " = " + quoteIdentifier("T2") + "." + quoteIdentifier(col) : quoteIdentifier("T1") + "." + quoteIdentifier(col) + " =nvl(" + quoteIdentifier("T2") + "." + quoteIdentifier(col) + "," + quoteIdentifier("T1") + "." + quoteIdentifier(col) + ")";
        return conncetionSql;
    }


    private static String buildConnectionConditions(List<String> uniqueKeyFields) {
        return uniqueKeyFields.stream().map((col) -> {
            return "T1." + quoteIdentifier(col.trim()) + "=T2." + quoteIdentifier(col.trim());
        }).collect(Collectors.joining(" and "));
    }


}
